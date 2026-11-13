const { afterEach, describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { PublicadorDaOutbox } = require('../dist/src/messaging/outbox-publisher.js');

const publishers = [];
const previousQueueUrl = process.env.SQS_EVENTS_QUEUE_URL;

function criarEvento(indice, carteiraId = randomUUID()) {
  return {
    id: randomUUID(),
    aggregate_id: carteiraId,
    event_type: 'WagerTransactionProcessed',
    payload: { indice },
    occurred_at: new Date('2026-11-13T12:00:00.000Z'),
    correlation_id: null,
    causation_id: null,
    attempts: 1,
  };
}

function criarBancoFalso(eventos, { falharConfirmacao = false, reservarUmPorCarteira = false } = {}) {
  const chamadasDeReserva = [];
  const chamadasDeConfirmacao = [];
  let fila = [...eventos];
  const orm = {
    em: {
      transactional: async (operacao) => operacao({
        execute: async (sql, parametros) => {
          chamadasDeReserva.push({ sql, parametros });
          let reservados;
          if (reservarUmPorCarteira) {
            const carteirasReservadas = new Set();
            reservados = [];
            fila = fila.filter((evento) => {
              if (reservados.length === 40 || carteirasReservadas.has(evento.aggregate_id)) return true;
              carteirasReservadas.add(evento.aggregate_id);
              reservados.push(evento);
              return false;
            });
          } else {
            reservados = fila.splice(0, 40);
          }
          return reservados;
        },
      }),
      getConnection: () => ({
        execute: async (sql, parametros) => {
          chamadasDeConfirmacao.push({ sql, parametros });
          if (falharConfirmacao) throw new Error('falha simulada de confirmação no PostgreSQL');
          return [];
        },
      }),
    },
  };
  return { orm, chamadasDeReserva, chamadasDeConfirmacao };
}

function criarPublicador(banco, enviarMensagem) {
  process.env.SQS_EVENTS_QUEUE_URL = 'http://localstack.test/queue.fifo';
  const publicador = new PublicadorDaOutbox(banco.orm);
  const clienteSqs = {
    send: enviarMensagem,
    destroy() {},
  };
  Object.defineProperty(publicador, 'client', { configurable: true, value: clienteSqs });
  publishers.push(publicador);
  return publicador;
}

async function aguardarAte(condicao, limiteMs = 3000) {
  const prazoFinal = Date.now() + limiteMs;
  while (Date.now() < prazoFinal) {
    if (condicao()) return;
    await new Promise((resolver) => setTimeout(resolver, 5));
  }
  assert.fail('A operação assíncrona não terminou dentro do prazo do teste');
}

async function iniciarEAguardarConfirmacao(publicador, banco, quantidade = 1) {
  publicador.onModuleInit();
  await aguardarAte(() => banco.chamadasDeConfirmacao.length >= quantidade);
  await publicador.onModuleDestroy();
}

afterEach(async () => {
  await Promise.all(publishers.splice(0).map((publicador) => publicador.onModuleDestroy()));
  if (previousQueueUrl === undefined) delete process.env.SQS_EVENTS_QUEUE_URL;
  else process.env.SQS_EVENTS_QUEUE_URL = previousQueueUrl;
});

describe('PublicadorDaOutbox', () => {
  it('envia 40 eventos em quatro lotes FIFO e confirma todos com uma escrita SQL', async () => {
    const eventos = Array.from({ length: 40 }, (_, indice) => criarEvento(indice));
    const banco = criarBancoFalso(eventos);
    const tamanhosDosLotes = [];
    const propriedadesDasEntradas = [];
    let enviosConcorrentes = 0;
    let maximoDeEnviosConcorrentes = 0;
    const publicador = criarPublicador(banco, async (comando) => {
      const entradas = comando.input.Entries;
      tamanhosDosLotes.push(entradas.length);
      propriedadesDasEntradas.push(...entradas.map((entrada) => ({
        eventId: JSON.parse(entrada.MessageBody).eventId,
        deduplicationId: entrada.MessageDeduplicationId,
        groupId: entrada.MessageGroupId,
      })));
      enviosConcorrentes++;
      maximoDeEnviosConcorrentes = Math.max(maximoDeEnviosConcorrentes, enviosConcorrentes);
      await new Promise((resolver) => setTimeout(resolver, 10));
      enviosConcorrentes--;
      return { Successful: entradas.map(({ Id }) => ({ Id })), Failed: [] };
    });

    await iniciarEAguardarConfirmacao(publicador, banco);

    assert.deepEqual(tamanhosDosLotes.sort(), [10, 10, 10, 10]);
    assert.equal(maximoDeEnviosConcorrentes, 4);
    assert.equal(banco.chamadasDeConfirmacao.length, 1);
    const sqlDaConfirmacao = banco.chamadasDeConfirmacao[0].sql;
    assert.match(sqlDaConfirmacao, /from \(values /i);
    assert.match(sqlDaConfirmacao, /lease_token/);
    assert.match(banco.chamadasDeReserva[0].sql, /anterior\.seq < candidato\.seq/);
    const entradas = banco.chamadasDeConfirmacao[0].parametros;
    assert.equal(entradas.length, 160);
    assert.deepEqual(entradas.filter((_, indice) => indice % 4 === 2), Array(40).fill(true));
    assert.ok(propriedadesDasEntradas.every(({ eventId, deduplicationId }) => eventId === deduplicationId));
    for (const entrada of propriedadesDasEntradas) {
      assert.equal(entrada.groupId, eventos.find(({ id }) => id === entrada.eventId).aggregate_id);
    }
  });

  it('trata rejeições parciais mesmo quando o SQS aceita a chamada em lote', async () => {
    const eventos = Array.from({ length: 3 }, (_, indice) => criarEvento(indice));
    const banco = criarBancoFalso(eventos);
    const publicador = criarPublicador(banco, async (comando) => ({
      Successful: comando.input.Entries.slice(1).map(({ Id }) => ({ Id })),
      Failed: [{ Id: comando.input.Entries[0].Id, Code: 'Throttling', SenderFault: false }],
    }));

    await iniciarEAguardarConfirmacao(publicador, banco);

    const parametros = banco.chamadasDeConfirmacao[0].parametros;
    assert.equal(parametros.length, 12);
    assert.deepEqual([parametros[2], parametros[6], parametros[10]], [false, true, true]);
    assert.ok(parametros[3] > 0);
    assert.match(banco.chamadasDeConfirmacao[0].sql, /next_attempt_at/);
  });

  it('respeita o limite de bytes do SQS e retém uma mensagem grande', async () => {
    const eventos = [criarEvento(1), criarEvento(2), criarEvento(3)];
    eventos[0].payload = { texto: 'x'.repeat(600_000) };
    eventos[1].payload = { texto: 'y'.repeat(600_000) };
    eventos[2].payload = { texto: 'z'.repeat(1_048_577) };
    const banco = criarBancoFalso(eventos);
    const tamanhosDosLotes = [];
    const publicador = criarPublicador(banco, async (comando) => {
      tamanhosDosLotes.push(comando.input.Entries.length);
      return { Successful: comando.input.Entries.map(({ Id }) => ({ Id })), Failed: [] };
    });

    await iniciarEAguardarConfirmacao(publicador, banco);

    assert.deepEqual(tamanhosDosLotes.sort(), [1, 1]);
    const parametros = banco.chamadasDeConfirmacao[0].parametros;
    assert.deepEqual([parametros[2], parametros[6], parametros[10]], [false, true, true]);
  });

  it('drena várias rodadas sem inverter eventos da mesma carteira', async () => {
    const carteiraId = randomUUID();
    const primeiroEvento = criarEvento(1, carteiraId);
    const segundoEvento = criarEvento(2, carteiraId);
    const banco = criarBancoFalso([primeiroEvento, segundoEvento], { reservarUmPorCarteira: true });
    const idsEnviados = [];
    const publicador = criarPublicador(banco, async (comando) => {
      const ids = comando.input.Entries.map(({ Id }) => Id);
      idsEnviados.push(...ids);
      return { Successful: ids.map((Id) => ({ Id })), Failed: [] };
    });

    await iniciarEAguardarConfirmacao(publicador, banco, 2);

    assert.deepEqual(idsEnviados, [primeiroEvento.id, segundoEvento.id]);
    assert.equal(banco.chamadasDeConfirmacao.length, 2);
    assert.match(banco.chamadasDeReserva[0].sql, /anterior\.aggregate_id = candidato\.aggregate_id/);
  });

  it('dois publicadores concorrentes reservam eventos distintos em lotes de até 40', async () => {
    const eventos = Array.from({ length: 60 }, (_, indice) => criarEvento(indice));
    const banco = criarBancoFalso(eventos);
    const idsEnviados = [];
    const enviar = async (comando) => {
      const ids = comando.input.Entries.map(({ Id }) => Id);
      idsEnviados.push(...ids);
      return { Successful: ids.map((Id) => ({ Id })), Failed: [] };
    };
    const primeiros = criarPublicador(banco, enviar);
    const segundos = criarPublicador(banco, enviar);
    primeiros.onModuleInit();
    segundos.onModuleInit();
    await aguardarAte(() => banco.chamadasDeConfirmacao.length === 2);
    await Promise.all([primeiros.onModuleDestroy(), segundos.onModuleDestroy()]);

    assert.equal(banco.chamadasDeReserva.length >= 2, true);
    assert.equal(idsEnviados.length, 60);
    assert.equal(new Set(idsEnviados).size, 60);
    assert.ok(banco.chamadasDeReserva.every(({ sql }) => /skip locked/i.test(sql)));
  });

  it('mantém o identificador do evento estável após falha na confirmação SQL', async () => {
    const evento = criarEvento(1);
    const primeiraBase = criarBancoFalso([evento], { falharConfirmacao: true });
    const idsEnviados = [];
    const primeiroPublicador = criarPublicador(primeiraBase, async (comando) => {
      const entrada = comando.input.Entries[0];
      idsEnviados.push({ eventId: JSON.parse(entrada.MessageBody).eventId, deduplicationId: entrada.MessageDeduplicationId });
      return { Successful: [{ Id: entrada.Id }], Failed: [] };
    });
    await iniciarEAguardarConfirmacao(primeiroPublicador, primeiraBase);

    const segundaBase = criarBancoFalso([evento]);
    const segundoPublicador = criarPublicador(segundaBase, async (comando) => {
      const entrada = comando.input.Entries[0];
      idsEnviados.push({ eventId: JSON.parse(entrada.MessageBody).eventId, deduplicationId: entrada.MessageDeduplicationId });
      return { Successful: [{ Id: entrada.Id }], Failed: [] };
    });
    await iniciarEAguardarConfirmacao(segundoPublicador, segundaBase);

    assert.deepEqual(idsEnviados, [
      { eventId: evento.id, deduplicationId: evento.id },
      { eventId: evento.id, deduplicationId: evento.id },
    ]);
    assert.match(primeiraBase.chamadasDeReserva[0].sql, /lease_until < now\(\)/);
  });

  it('permite recuperar o evento após expirar a locação de confirmação', async () => {
    const evento = criarEvento(1);
    const banco = criarBancoFalso([evento], { falharConfirmacao: true });
    const publicador = criarPublicador(banco, async () => ({ Successful: [{ Id: evento.id }], Failed: [] }));
    const mensagensDeErro = [];
    Object.defineProperty(publicador, 'logger', {
      configurable: true,
      value: { error: (mensagem) => mensagensDeErro.push(mensagem), warn() {} },
    });
    await iniciarEAguardarConfirmacao(publicador, banco);

    assert.equal(banco.chamadasDeConfirmacao.length, 1);
    assert.match(banco.chamadasDeReserva[0].sql, /lease_until = now\(\) \+ interval '30 seconds'/);
    assert.equal(banco.chamadasDeConfirmacao[0].parametros[2], true);
    assert.equal(mensagensDeErro.some((mensagem) => mensagem.includes('falha simulada de confirmação')), false);
    assert.match(mensagensDeErro[0], /errorName/);
  });
});
