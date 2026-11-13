import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { MikroORM } from '@mikro-orm/postgresql';
import {
  CreateQueueCommand,
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';

const { PublicadorDaOutbox } = require('../../dist/src/messaging/outbox-publisher.js');
const quantidadeDeEventos = 45;
let orm;
let clienteSqs;
let urlDaFila;
let identificadoresDosEventos = [];
let carteiraComEventosEmOrdem;
let nomeDoGatilhoDeFalha;
let nomeDaFuncaoDeFalha;
const identificadoresParaLimpeza = new Set();
const publicadoresAtivos = [];

async function esperarAte(condicao, limiteMs = 30_000) {
  const prazoFinal = Date.now() + limiteMs;
  while (Date.now() < prazoFinal) {
    if (await condicao()) return;
    await new Promise((resolver) => setTimeout(resolver, 25));
  }
  throw new Error('Tempo limite aguardando o publisher da outbox');
}

async function consultarQuantidadeDeEventosPublicados() {
  const marcadores = identificadoresDosEventos.map(() => '?').join(',');
  const [linha] = await orm.em.getConnection().execute(`
    select count(*)::int as quantidade from outbox_message
    where id in (${marcadores}) and published_at is not null
  `, identificadoresDosEventos);
  return Number(linha.quantidade);
}

beforeAll(async () => {
  if (!process.env.TEST_DATABASE_URL || !process.env.SQS_ENDPOINT_URL) {
    throw new Error('Defina TEST_DATABASE_URL e SQS_ENDPOINT_URL para PostgreSQL e LocalStack descartáveis.');
  }
  process.env.DATABASE_URL = process.env.TEST_DATABASE_URL;
  const endpoint = process.env.SQS_ENDPOINT_URL;
  clienteSqs = new SQSClient({
    region: process.env.SQS_REGION ?? 'us-east-1',
    endpoint,
    credentials: { accessKeyId: process.env.SQS_ACCESS_KEY ?? 'test', secretAccessKey: process.env.SQS_SECRET_KEY ?? 'test' },
  });
  const nomeDaFila = `outbox-test-${randomUUID().slice(0, 8)}.fifo`;
  const criada = await clienteSqs.send(new CreateQueueCommand({
    QueueName: nomeDaFila,
    Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
  }));
  urlDaFila = `${endpoint.replace(/\/$/, '')}${new URL(criada.QueueUrl).pathname}`;
  process.env.SQS_EVENTS_QUEUE_URL = urlDaFila;

  const configuracao = (await import('../../dist/mikro-orm.config.js')).default;
  orm = await MikroORM.init(configuracao);

  carteiraComEventosEmOrdem = randomUUID();
  const eventos = Array.from({ length: quantidadeDeEventos }, (_, indice) => ({
    id: randomUUID(),
    aggregate_id: indice < 2 ? carteiraComEventosEmOrdem : randomUUID(),
    event_type: 'OutboxIntegrationTest',
    payload: { indice },
  }));
  identificadoresDosEventos = eventos.map(({ id }) => id);
  identificadoresDosEventos.forEach((id) => identificadoresParaLimpeza.add(id));
  await orm.em.getConnection().execute(`
    insert into outbox_message(id, aggregate_id, event_type, payload, occurred_at)
    select amostra.id, amostra.aggregate_id, amostra.event_type, amostra.payload, now()
    from jsonb_to_recordset(?::jsonb)
      as amostra(id uuid, aggregate_id uuid, event_type varchar, payload jsonb)
  `, [JSON.stringify(eventos)]);

  // Confirma que uma locação vencida também volta a ser elegível para publicação.
  await orm.em.getConnection().execute(`
    update outbox_message set lease_token=?, lease_until=now()-interval '1 second' where id=?
  `, [randomUUID(), eventos[2].id]);
}, 30_000);

afterAll(async () => {
  await Promise.all(publicadoresAtivos.splice(0).map((publicador) => publicador.onModuleDestroy()));
  if (nomeDoGatilhoDeFalha && orm) {
    await orm.em.getConnection().execute(`drop trigger if exists ${nomeDoGatilhoDeFalha} on outbox_message`);
  }
  if (nomeDaFuncaoDeFalha && orm) {
    await orm.em.getConnection().execute(`drop function if exists ${nomeDaFuncaoDeFalha}()`);
  }
  const idsParaRemover = [...identificadoresParaLimpeza];
  if (idsParaRemover.length && orm) {
    const marcadores = idsParaRemover.map(() => '?').join(',');
    await orm.em.getConnection().execute(`delete from outbox_message where id in (${marcadores})`, idsParaRemover);
  }
  if (urlDaFila && clienteSqs) await clienteSqs.send(new DeleteQueueCommand({ QueueUrl: urlDaFila })).catch(() => {});
  clienteSqs?.destroy();
  if (orm) await orm.close(true);
});

test('publica em lotes concorrentes, recupera lease vencido e conserva ordem FIFO por carteira', async () => {
  const primeiroPublicador = new PublicadorDaOutbox(orm);
  const segundoPublicador = new PublicadorDaOutbox(orm);
  publicadoresAtivos.push(primeiroPublicador, segundoPublicador);
  primeiroPublicador.onModuleInit();
  segundoPublicador.onModuleInit();

  await esperarAte(async () => (await consultarQuantidadeDeEventosPublicados()) === quantidadeDeEventos);
  await Promise.all([primeiroPublicador.onModuleDestroy(), segundoPublicador.onModuleDestroy()]);
  publicadoresAtivos.length = 0;

  const mensagens = [];
  const identificadoresEsperados = new Set(identificadoresDosEventos);
  while (identificadoresDosEventos.some((id) => !mensagens.some(({ eventId }) => eventId === id))) {
    const resposta = await clienteSqs.send(new ReceiveMessageCommand({
      QueueUrl: urlDaFila,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 1,
      VisibilityTimeout: 10,
    }));
    for (const mensagem of resposta.Messages ?? []) {
      mensagens.push(JSON.parse(mensagem.Body));
      await clienteSqs.send(new DeleteMessageCommand({ QueueUrl: urlDaFila, ReceiptHandle: mensagem.ReceiptHandle }));
    }
  }

  const mensagensDaExecucao = mensagens.filter(({ eventId }) => identificadoresEsperados.has(eventId));
  expect(new Set(mensagensDaExecucao.map(({ eventId }) => eventId)).size).toBe(quantidadeDeEventos);
  expect(mensagensDaExecucao).toHaveLength(quantidadeDeEventos);
  const eventosDaMesmaCarteira = mensagens
    .filter(({ aggregateId }) => aggregateId === carteiraComEventosEmOrdem)
    .map(({ data }) => data.indice);
  expect(eventosDaMesmaCarteira).toEqual([0, 1]);
});

test('recupera após falha PostgreSQL na confirmação e reenvia o mesmo eventId', async () => {
  const evento = {
    id: randomUUID(),
    aggregate_id: randomUUID(),
    event_type: 'OutboxConfirmationFailureTest',
    payload: { tentativa: 'recovery' },
  };
  identificadoresDosEventos = [evento.id];
  identificadoresParaLimpeza.add(evento.id);
  await orm.em.getConnection().execute(`
    insert into outbox_message(id, aggregate_id, event_type, payload, occurred_at)
    values(?, ?, ?, ?::jsonb, now())
  `, [evento.id, evento.aggregate_id, evento.event_type, JSON.stringify(evento.payload)]);

  const sufixoSeguro = randomUUID().replaceAll('-', '');
  nomeDaFuncaoDeFalha = `interromper_confirmacao_${sufixoSeguro}`;
  nomeDoGatilhoDeFalha = `interromper_confirmacao_${sufixoSeguro}`;
  await orm.em.getConnection().execute(`
    create function ${nomeDaFuncaoDeFalha}() returns trigger language plpgsql as $$
    begin
      if new.published_at is not null and old.published_at is null then
        raise exception 'falha PostgreSQL injetada pelo teste';
      end if;
      return new;
    end $$;
    create trigger ${nomeDoGatilhoDeFalha} before update on outbox_message
      for each row execute function ${nomeDaFuncaoDeFalha}();
  `);

  let primeiraMensagem;
  const primeiroPublicador = new PublicadorDaOutbox(orm);
  publicadoresAtivos.push(primeiroPublicador);
  primeiroPublicador.onModuleInit();
  await esperarAte(async () => {
    const recebida = await clienteSqs.send(new ReceiveMessageCommand({
      QueueUrl: urlDaFila,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 1,
      VisibilityTimeout: 10,
    }));
    primeiraMensagem = recebida.Messages?.find((mensagem) => JSON.parse(mensagem.Body).eventId === evento.id);
    return Boolean(primeiraMensagem);
  });
  await primeiroPublicador.onModuleDestroy();
  publicadoresAtivos.length = 0;

  const [estadoAposFalha] = await orm.em.getConnection().execute(
    'select published_at, lease_token from outbox_message where id=?', [evento.id],
  );
  expect(estadoAposFalha.published_at).toBeNull();
  expect(estadoAposFalha.lease_token).not.toBeNull();

  await orm.em.getConnection().execute(`drop trigger if exists ${nomeDoGatilhoDeFalha} on outbox_message`);
  await orm.em.getConnection().execute(`drop function if exists ${nomeDaFuncaoDeFalha}()`);
  nomeDoGatilhoDeFalha = undefined;
  nomeDaFuncaoDeFalha = undefined;
  await orm.em.getConnection().execute(`
    update outbox_message set lease_until=now()-interval '1 second' where id=?
  `, [evento.id]);

  const segundoPublicador = new PublicadorDaOutbox(orm);
  publicadoresAtivos.push(segundoPublicador);
  segundoPublicador.onModuleInit();
  await esperarAte(async () => {
    const [linha] = await orm.em.getConnection().execute(
      'select published_at from outbox_message where id=?', [evento.id],
    );
    return linha.published_at !== null;
  });
  await segundoPublicador.onModuleDestroy();
  publicadoresAtivos.length = 0;

  await clienteSqs.send(new ChangeMessageVisibilityCommand({
    QueueUrl: urlDaFila,
    ReceiptHandle: primeiraMensagem.ReceiptHandle,
    VisibilityTimeout: 0,
  }));
  const mensagensDoMesmoEvento = [];
  const prazoFinal = Date.now() + 3000;
  while (Date.now() < prazoFinal) {
    const recebidas = await clienteSqs.send(new ReceiveMessageCommand({
      QueueUrl: urlDaFila,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 1,
      VisibilityTimeout: 10,
    }));
    for (const mensagem of recebidas.Messages ?? []) {
      if (JSON.parse(mensagem.Body).eventId === evento.id) mensagensDoMesmoEvento.push(mensagem);
      await clienteSqs.send(new DeleteMessageCommand({ QueueUrl: urlDaFila, ReceiptHandle: mensagem.ReceiptHandle }));
    }
    if (!recebidas.Messages?.length) break;
  }
  expect(mensagensDoMesmoEvento).toHaveLength(1);
});
