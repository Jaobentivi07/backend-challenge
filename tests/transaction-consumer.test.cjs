const { afterEach, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  SendMessageCommand,
} = require('@aws-sdk/client-sqs');
const { ConsumidorDeTransacoes } = require('../dist/src/messaging/transaction-consumer.js');

const chavesDeAmbiente = ['SQS_TRANSACTIONS_QUEUE_URL', 'SQS_TRANSACTIONS_DLQ_URL', 'SQS_ENDPOINT_URL'];
const ambienteAnterior = Object.fromEntries(chavesDeAmbiente.map((chave) => [chave, process.env[chave]]));
const buscarOriginal = global.fetch;

function criarConsumidorFake() {
  process.env.SQS_TRANSACTIONS_QUEUE_URL = 'http://sqs.test/transactions.fifo';
  process.env.SQS_TRANSACTIONS_DLQ_URL = 'http://sqs.test/transactions-dlq.fifo';
  delete process.env.SQS_ENDPOINT_URL;
  const consumidor = new ConsumidorDeTransacoes();
  const comandosEnviados = [];
  Object.defineProperty(consumidor, 'client', {
    configurable: true,
    value: { send: async (comando) => { comandosEnviados.push(comando); return {}; }, destroy() {} },
  });
  return { consumidor, comandosEnviados };
}

afterEach(() => {
  for (const chave of chavesDeAmbiente) {
    if (ambienteAnterior[chave] === undefined) delete process.env[chave];
    else process.env[chave] = ambienteAnterior[chave];
  }
  global.fetch = buscarOriginal;
});

it('exige URL da DLQ antes de iniciar o consumidor', () => {
  process.env.SQS_TRANSACTIONS_QUEUE_URL = 'http://sqs.test/transactions.fifo';
  delete process.env.SQS_TRANSACTIONS_DLQ_URL;
  delete process.env.SQS_ENDPOINT_URL;
  const consumidor = new ConsumidorDeTransacoes();
  assert.throws(() => consumidor.onModuleInit(), /SQS_TRANSACTIONS_DLQ_URL is required/);
});

it('encaminha envelope inválido para a DLQ antes de confirmar a mensagem original', async () => {
  const { consumidor, comandosEnviados } = criarConsumidorFake();
  await consumidor.processarMensagem({
    Body: '{malformed',
    MessageId: 'mensagem-1',
    ReceiptHandle: 'recibo-1',
  });

  assert.equal(comandosEnviados.length, 2);
  assert.ok(comandosEnviados[0] instanceof SendMessageCommand);
  assert.equal(JSON.parse(comandosEnviados[0].input.MessageBody).reason, 'invalid_envelope');
  assert.ok(comandosEnviados[1] instanceof DeleteMessageCommand);
});

it('reagenda falha HTTP transitória e confirma resposta terminal sem DLQ', async () => {
  const { consumidor, comandosEnviados } = criarConsumidorFake();
  let cabecalhosEnviados;
  global.fetch = async (_url, opcoes) => {
    cabecalhosEnviados = opcoes.headers;
    return { status: 503 };
  };
  await consumidor.processarMensagem({
    Body: JSON.stringify({ messageId: 'mensagem-2', type: 'WagerTransactionRequested', data: { idempotencyKey: 'chave' } }),
    MessageId: 'mensagem-2',
    ReceiptHandle: 'recibo-2',
    Attributes: { ApproximateReceiveCount: '2' },
  });
  assert.ok(comandosEnviados[0] instanceof ChangeMessageVisibilityCommand);
  assert.equal(cabecalhosEnviados['x-sqs-payload-hash'].length, 64);
  assert.equal(cabecalhosEnviados['x-sqs-message-id'], 'mensagem-2');

  comandosEnviados.length = 0;
  global.fetch = async () => ({ status: 422 });
  await consumidor.processarMensagem({
    Body: JSON.stringify({ messageId: 'mensagem-3', type: 'WagerTransactionRequested', data: { idempotencyKey: 'chave' } }),
    MessageId: 'mensagem-3',
    ReceiptHandle: 'recibo-3',
  });
  assert.equal(comandosEnviados.length, 1);
  assert.ok(comandosEnviados[0] instanceof DeleteMessageCommand);
});
