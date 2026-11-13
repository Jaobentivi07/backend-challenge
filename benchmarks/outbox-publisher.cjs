/* Benchmark real do PostgreSQL e LocalStack. Use apenas banco descartável. */
const { performance } = require('node:perf_hooks');
const { randomUUID } = require('node:crypto');
const { MikroORM } = require('@mikro-orm/postgresql');
const {
  CreateQueueCommand,
  DeleteQueueCommand,
  SendMessageCommand,
  SQSClient,
} = require('@aws-sdk/client-sqs');
const { PublicadorDaOutbox } = require('../dist/src/messaging/outbox-publisher.js');

const quantidadeDeEventos = Number(process.env.BENCH_EVENTS ?? 200);
const espera = (milissegundos) => new Promise((resolver) => setTimeout(resolver, milissegundos));

function percentil(amostras, fracao) {
  if (!amostras.length) return null;
  const ordenadas = [...amostras].sort((a, b) => a - b);
  return Number(ordenadas[Math.ceil(fracao * ordenadas.length) - 1].toFixed(2));
}

async function criarEventos(orm, execucao) {
  const ids = Array.from({ length: quantidadeDeEventos }, () => randomUUID());
  const carteiras = ids.map(() => randomUUID());
  const eventos = ids.map((eventId, indice) => ({ event_id: eventId, aggregate_id: carteiras[indice], indice: indice + 1 }));
  await orm.em.getConnection().execute(`
    insert into outbox_message(id, aggregate_id, event_type, payload, occurred_at)
    select amostra.event_id, amostra.aggregate_id, 'OutboxBenchmark',
           jsonb_build_object('execucao', ?, 'indice', amostra.indice), now()
    from jsonb_to_recordset(?::jsonb) as amostra(event_id uuid, aggregate_id uuid, indice integer)
  `, [execucao, JSON.stringify(eventos)]);
  return ids;
}

async function medirBacklog(orm, ids) {
  const marcadores = ids.map(() => '?').join(',');
  const [linha] = await orm.em.getConnection().execute(`
    select count(*)::int as quantidade,
           coalesce(extract(epoch from (now() - min(created_at))), 0)::float as idade_segundos
    from outbox_message where id in (${marcadores}) and published_at is null
  `, ids);
  return { quantidade: Number(linha.quantidade), idadeSegundos: Number(linha.idade_segundos) };
}

function envelope(evento) {
  return JSON.stringify({
    eventId: evento.id,
    eventType: evento.event_type,
    aggregateId: evento.aggregate_id,
    correlationId: evento.correlation_id ?? evento.id,
    ...(evento.causation_id ? { causationId: evento.causation_id } : {}),
    occurredAt: new Date(evento.occurred_at).toISOString(),
    version: 1,
    data: evento.payload,
  });
}

async function medirVersaoOriginal(orm, clienteSqs, urlDaFila, ids) {
  const latenciasDeEnvio = [];
  const latenciasNaFila = [];
  let chamadasSqs = 0;
  let confirmacoesSql = 0;
  const inicio = performance.now();
  let publicados = 0;

  while (publicados < ids.length) {
    const token = randomUUID();
    const marcadoresDosIds = ids.map(() => '?').join(',');
    const eventos = await orm.em.transactional(async (gerenteTransacional) => gerenteTransacional.execute(`
      with elegiveis as (
        select candidato.id from outbox_message candidato
        where candidato.id in (${marcadoresDosIds}) and candidato.published_at is null
          and (candidato.next_attempt_at is null or candidato.next_attempt_at <= now())
          and (candidato.lease_until is null or candidato.lease_until < now())
          and not exists (select 1 from outbox_message anterior
            where anterior.aggregate_id=candidato.aggregate_id and anterior.published_at is null
              and anterior.seq<candidato.seq)
        order by candidato.seq limit 25 for update skip locked
      )
      update outbox_message evento set lease_token=?, lease_until=now()+interval '30 seconds',
        attempts=evento.attempts+1 from elegiveis where evento.id=elegiveis.id
      returning evento.*
    `, [...ids, token]));

    for (const evento of eventos) {
      const inicioDoEnvio = performance.now();
      await clienteSqs.send(new SendMessageCommand({
        QueueUrl: urlDaFila,
        MessageBody: envelope(evento),
        MessageGroupId: evento.aggregate_id,
        MessageDeduplicationId: evento.id,
      }));
      chamadasSqs++;
      confirmacoesSql++;
      await orm.em.getConnection().execute(`update outbox_message
        set published_at=now(), lease_token=null, lease_until=null where id=? and lease_token=?`, [evento.id, token]);
      const momentoDaConfirmacao = performance.now();
      latenciasDeEnvio.push(momentoDaConfirmacao - inicioDoEnvio);
      latenciasNaFila.push(Date.now() - new Date(evento.occurred_at).getTime());
      publicados++;
    }
    if (publicados < ids.length) await espera(1000);
  }

  return resumir('ORIGINAL_REAL', inicio, ids.length, latenciasDeEnvio, latenciasNaFila, chamadasSqs, confirmacoesSql, 0);
}

async function medirVersaoOtimizada(orm, urlDaFila, ids) {
  process.env.SQS_EVENTS_QUEUE_URL = urlDaFila;
  const publicador = new PublicadorDaOutbox(orm);
  const clienteSqs = publicador.client;
  const enviarOriginal = clienteSqs.send.bind(clienteSqs);
  const obterConexaoOriginal = orm.em.getConnection.bind(orm.em);
  const momentosDeEnvio = new Map();
  const latenciasDeEnvio = [];
  const latenciasNaFila = [];
  const conexoesInstrumentadas = new WeakSet();
  let chamadasSqs = 0;
  let confirmacoesSql = 0;
  let falhasSqs = 0;
  let falhasConfirmacaoSql = 0;
  let inicio;

  clienteSqs.send = async (comando) => {
    if (comando.input.Entries) {
      chamadasSqs++;
      for (const entrada of comando.input.Entries) {
        const occurredAt = JSON.parse(entrada.MessageBody).occurredAt;
        momentosDeEnvio.set(entrada.Id, { inicioDoEnvio: performance.now(), ocorridoEm: Date.parse(occurredAt) });
      }
      try {
        const resposta = await enviarOriginal(comando);
        falhasSqs += resposta.Failed?.length ?? 0;
        return resposta;
      } catch (erro) {
        falhasSqs += comando.input.Entries.length;
        throw erro;
      }
    }
    return enviarOriginal(comando);
  };

  orm.em.getConnection = () => {
    const conexao = obterConexaoOriginal();
    if (conexoesInstrumentadas.has(conexao)) return conexao;
    conexoesInstrumentadas.add(conexao);
    const executarOriginal = conexao.execute.bind(conexao);
    conexao.execute = async (sql, parametros) => {
      if (sql.toLowerCase().includes('from (values') && sql.includes('lease_token')) {
        confirmacoesSql++;
        try {
          const resultado = await executarOriginal(sql, parametros);
          for (let indice = 0; indice < parametros.length; indice += 4) {
            const eventId = parametros[indice];
            const confirmado = parametros[indice + 2];
            if (confirmado && momentosDeEnvio.has(eventId)) {
              const momentoDaConfirmacao = performance.now();
              const momentos = momentosDeEnvio.get(eventId);
              latenciasDeEnvio.push(momentoDaConfirmacao - momentos.inicioDoEnvio);
              latenciasNaFila.push(Date.now() - momentos.ocorridoEm);
              momentosDeEnvio.delete(eventId);
            }
          }
          return resultado;
        } catch (erro) {
          falhasConfirmacaoSql++;
          throw erro;
        }
      }
      return executarOriginal(sql, parametros);
    };
    return conexao;
  };

  inicio = performance.now();
  publicador.onModuleInit();
  const prazo = Date.now() + 180_000;
  const marcadoresDosIds = ids.map(() => '?').join(',');
  while (Date.now() < prazo) {
    const [linha] = await obterConexaoOriginal().execute(`
      select count(*)::int as quantidade from outbox_message
      where id in (${marcadoresDosIds}) and published_at is not null
    `, ids);
    if (Number(linha.quantidade) === ids.length) break;
    await espera(20);
  }
  await publicador.onModuleDestroy();
  orm.em.getConnection = obterConexaoOriginal;

  const [linhaFinal] = await obterConexaoOriginal().execute(`
    select count(*)::int as quantidade from outbox_message
    where id in (${marcadoresDosIds}) and published_at is not null
  `, ids);
  if (Number(linhaFinal.quantidade) !== ids.length) throw new Error('A outbox otimizada não drenou todos os eventos em 180 segundos');
  return resumir('OTIMIZADA_REAL', inicio, ids.length, latenciasDeEnvio, latenciasNaFila, chamadasSqs, confirmacoesSql, falhasSqs, falhasConfirmacaoSql);
}

function resumir(modo, inicio, eventosPublicados, latenciasDeEnvio, latenciasNaFila, chamadasSqs, confirmacoesSql, falhasSqs, falhasConfirmacaoSql = 0) {
  const duracaoMs = performance.now() - inicio;
  return {
    modo,
    eventosPublicados,
    eventosPorSegundo: Number((eventosPublicados * 1000 / duracaoMs).toFixed(2)),
    tempoTotalMs: Number(duracaoMs.toFixed(2)),
    latenciaEnvioAConfirmacaoMs: { p50: percentil(latenciasDeEnvio, 0.50), p95: percentil(latenciasDeEnvio, 0.95), p99: percentil(latenciasDeEnvio, 0.99) },
    idadeDoEventoAConfirmacaoMs: { p50: percentil(latenciasNaFila, 0.50), p95: percentil(latenciasNaFila, 0.95), p99: percentil(latenciasNaFila, 0.99) },
    chamadasSqs: chamadasSqs,
    confirmacoesSql,
    falhasSqs,
    retries: falhasSqs,
    falhasDeConfirmacaoSql: falhasConfirmacaoSql,
  };
}

async function main() {
  if (process.env.ALLOW_DISPOSABLE_DB !== 'YES') throw new Error('Defina ALLOW_DISPOSABLE_DB=YES somente para autorizar o uso de um PostgreSQL descartável.');
  if (!process.env.BENCH_DATABASE_URL || !process.env.SQS_ENDPOINT_URL) throw new Error('Defina BENCH_DATABASE_URL e SQS_ENDPOINT_URL para PostgreSQL e LocalStack descartáveis.');
  if (!Number.isInteger(quantidadeDeEventos) || quantidadeDeEventos < 1) throw new Error('BENCH_EVENTS deve ser um inteiro positivo.');

  process.env.DATABASE_URL = process.env.BENCH_DATABASE_URL;
  const config = require('../dist/mikro-orm.config.js').default;
  const orm = await MikroORM.init(config);
  const [backlogPendente] = await orm.em.getConnection().execute(
    'select count(*)::int as quantidade from outbox_message where published_at is null',
  );
  if (Number(backlogPendente.quantidade) > 0) {
    await orm.close(true);
    throw new Error(`Use um banco descartável sem eventos pendentes; encontrados ${backlogPendente.quantidade}.`);
  }
  const clienteSqs = new SQSClient({
    region: process.env.SQS_REGION ?? 'us-east-1',
    endpoint: process.env.SQS_ENDPOINT_URL,
    credentials: { accessKeyId: process.env.SQS_ACCESS_KEY ?? 'test', secretAccessKey: process.env.SQS_SECRET_KEY ?? 'test' },
  });
  const sufixo = randomUUID().slice(0, 8);
  const nomesDasFilas = [`bench-original-${sufixo}.fifo`, `bench-otimizada-${sufixo}.fifo`];
  const urlsDasFilas = [];
  const idsParaLimpeza = [];

  try {
    for (const QueueName of nomesDasFilas) {
      const criada = await clienteSqs.send(new CreateQueueCommand({ QueueName, Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' } }));
      const caminhoDaFila = new URL(criada.QueueUrl).pathname;
      urlsDasFilas.push(`${process.env.SQS_ENDPOINT_URL.replace(/\/$/, '')}${caminhoDaFila}`);
    }
    const idsOriginais = await criarEventos(orm, sufixo);
    idsParaLimpeza.push(...idsOriginais);
    const backlogOriginalAntes = await medirBacklog(orm, idsOriginais);
    const original = await medirVersaoOriginal(orm, clienteSqs, urlsDasFilas[0], idsOriginais);
    const backlogOriginalDepois = await medirBacklog(orm, idsOriginais);

    const idsOtimizados = await criarEventos(orm, `${sufixo}-optimized`);
    idsParaLimpeza.push(...idsOtimizados);
    const backlogOtimizadoAntes = await medirBacklog(orm, idsOtimizados);
    const otimizada = await medirVersaoOtimizada(orm, urlsDasFilas[1], idsOtimizados);
    const backlogOtimizadoDepois = await medirBacklog(orm, idsOtimizados);

    process.stdout.write(`${JSON.stringify({ classificacao: 'MEDIDO_REAL_POSTGRESQL_LOCALSTACK', eventosPorVersao: quantidadeDeEventos, original, otimizada, backlog: { original: { antes: backlogOriginalAntes, depois: backlogOriginalDepois }, otimizada: { antes: backlogOtimizadoAntes, depois: backlogOtimizadoDepois } } }, null, 2)}\n`);
  } finally {
    if (idsParaLimpeza.length) {
      const marcadores = idsParaLimpeza.map(() => '?').join(',');
      await orm.em.getConnection().execute(`delete from outbox_message where id in (${marcadores})`, idsParaLimpeza);
    }
    for (const QueueUrl of urlsDasFilas) await clienteSqs.send(new DeleteQueueCommand({ QueueUrl })).catch(() => {});
    clienteSqs.destroy();
    await orm.close(true);
  }
}

main().catch((erro) => {
  process.stderr.write(`${erro instanceof Error ? erro.stack : erro}\n`);
  process.exitCode = 1;
});
