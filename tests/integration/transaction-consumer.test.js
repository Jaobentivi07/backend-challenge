import { afterAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';

const { ConsumidorDeTransacoes } = require('../../dist/src/messaging/transaction-consumer.js');
const endpointSqs = process.env.SQS_ENDPOINT_URL;
let clienteDeTeste;
let consumidor;
let filaDeEntrada;
let filaDeErros;
const variaveisDeAmbienteAnteriores = {
  filaPrincipal: process.env.SQS_TRANSACTIONS_QUEUE_URL,
  filaDeErros: process.env.SQS_TRANSACTIONS_DLQ_URL,
  endpoint: process.env.SQS_ENDPOINT_URL,
};
const funcaoFetchOriginal = global.fetch;

async function receberMensagemDeTeste(urlDaFila, timeoutMs = 5_000) {
  const prazoFinal = Date.now() + timeoutMs;
  while (Date.now() < prazoFinal) {
    const resposta = await clienteDeTeste.send(new ReceiveMessageCommand({
      QueueUrl: urlDaFila,
      MaxNumberOfMessages: 1,
      WaitTimeSeconds: 1,
      VisibilityTimeout: 5,
      MessageSystemAttributeNames: ['ApproximateReceiveCount'],
    }));
    if (resposta.Messages?.[0]) return resposta.Messages[0];
  }
  throw new Error('Tempo limite aguardando mensagem no LocalStack');
}

afterAll(async () => {
  await consumidor?.onModuleDestroy();
  if (filaDeEntrada && clienteDeTeste) await clienteDeTeste.send(new DeleteQueueCommand({ QueueUrl: filaDeEntrada })).catch(() => {});
  if (filaDeErros && clienteDeTeste) await clienteDeTeste.send(new DeleteQueueCommand({ QueueUrl: filaDeErros })).catch(() => {});
  clienteDeTeste?.destroy();
  global.fetch = funcaoFetchOriginal;
  if (variaveisDeAmbienteAnteriores.filaPrincipal === undefined) delete process.env.SQS_TRANSACTIONS_QUEUE_URL;
  else process.env.SQS_TRANSACTIONS_QUEUE_URL = variaveisDeAmbienteAnteriores.filaPrincipal;
  if (variaveisDeAmbienteAnteriores.filaDeErros === undefined) delete process.env.SQS_TRANSACTIONS_DLQ_URL;
  else process.env.SQS_TRANSACTIONS_DLQ_URL = variaveisDeAmbienteAnteriores.filaDeErros;
  if (variaveisDeAmbienteAnteriores.endpoint === undefined) delete process.env.SQS_ENDPOINT_URL;
  else process.env.SQS_ENDPOINT_URL = variaveisDeAmbienteAnteriores.endpoint;
});

test('encaminha envelope inválido à DLQ real e torna a mensagem visível após falha transitória', async () => {
  if (!endpointSqs) throw new Error('Defina SQS_ENDPOINT_URL para executar os testes com LocalStack.');
  const regiao = process.env.SQS_REGION ?? 'us-east-1';
  const credenciais = {
    accessKeyId: process.env.SQS_ACCESS_KEY ?? 'test',
    secretAccessKey: process.env.SQS_SECRET_KEY ?? 'test',
  };
  clienteDeTeste = new SQSClient({ region: regiao, endpoint: endpointSqs, credentials: credenciais });

  const sufixoDaFila = randomUUID().slice(0, 8);
  const criadaFilaDeEntrada = await clienteDeTeste.send(new CreateQueueCommand({
    QueueName: `consumer-test-${sufixoDaFila}.fifo`,
    Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
  }));
  const criadaFilaDeErros = await clienteDeTeste.send(new CreateQueueCommand({
    QueueName: `consumer-test-dlq-${sufixoDaFila}.fifo`,
    Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
  }));
  filaDeEntrada = `${endpointSqs.replace(/\/$/, '')}${new URL(criadaFilaDeEntrada.QueueUrl).pathname}`;
  filaDeErros = `${endpointSqs.replace(/\/$/, '')}${new URL(criadaFilaDeErros.QueueUrl).pathname}`;
  process.env.SQS_TRANSACTIONS_QUEUE_URL = filaDeEntrada;
  process.env.SQS_TRANSACTIONS_DLQ_URL = filaDeErros;
  consumidor = new ConsumidorDeTransacoes();

  const identificadorDaMensagemInvalida = randomUUID();
  await clienteDeTeste.send(new SendMessageCommand({
    QueueUrl: filaDeEntrada,
    MessageBody: '{envelope inválido',
    MessageGroupId: 'testes-consumidor',
    MessageDeduplicationId: identificadorDaMensagemInvalida,
  }));
  const mensagemInvalida = await receberMensagemDeTeste(filaDeEntrada);
  await consumidor.processarMensagem(mensagemInvalida);

  const mensagemNaDlq = await receberMensagemDeTeste(filaDeErros);
  const envelopeDaDlq = JSON.parse(mensagemNaDlq.Body);
  expect(envelopeDaDlq.reason).toBe('invalid_envelope');
  expect(envelopeDaDlq.originalBody).toBe('{envelope inválido');
  expect(envelopeDaDlq.sourceMessageId).toBe(mensagemInvalida.MessageId);
  await clienteDeTeste.send(new DeleteMessageCommand({ QueueUrl: filaDeErros, ReceiptHandle: mensagemNaDlq.ReceiptHandle }));

  global.fetch = async () => ({ status: 503 });
  const envelopeDaTransacao = {
    messageId: randomUUID(),
    type: 'WagerTransactionRequested',
    data: { idempotencyKey: 'chave-de-retry' },
  };
  await clienteDeTeste.send(new SendMessageCommand({
    QueueUrl: filaDeEntrada,
    MessageBody: JSON.stringify(envelopeDaTransacao),
    MessageGroupId: 'testes-consumidor',
    MessageDeduplicationId: envelopeDaTransacao.messageId,
  }));
  const mensagemComFalhaTransitória = await receberMensagemDeTeste(filaDeEntrada);
  await consumidor.processarMensagem(mensagemComFalhaTransitória);

  const mensagemRecuperada = await receberMensagemDeTeste(filaDeEntrada, 5_000);
  expect(JSON.parse(mensagemRecuperada.Body)).toEqual(envelopeDaTransacao);
  expect(mensagemRecuperada.Attributes.ApproximateReceiveCount).toBe('2');
  await clienteDeTeste.send(new DeleteMessageCommand({ QueueUrl: filaDeEntrada, ReceiptHandle: mensagemRecuperada.ReceiptHandle }));
});
