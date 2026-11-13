import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { MikroORM } from '@mikro-orm/postgresql';
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';

const ports = [3311, 3312, 3313];
const children = [];
const urls = ports.map((port) => `http://127.0.0.1:${port}`);
const { ConsumidorDeTransacoes } = require('../../dist/src/messaging/transaction-consumer.js');

async function startServer(port) {
  const childEnv = { ...process.env, PORT: String(port) };
  delete childEnv.SQS_EVENTS_QUEUE_URL;
  delete childEnv.SQS_ENDPOINT_URL;
  const child = spawn(process.execPath, ['dist/src/main.js'], {
    cwd: process.cwd(),
    env: childEnv,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let logs = '';
  child.stdout.setEncoding('utf8').on('data', (chunk) => { logs = `${logs}${chunk}`.slice(-8000); });
  child.stderr.setEncoding('utf8').on('data', (chunk) => { logs = `${logs}${chunk}`.slice(-8000); });
  children.push(child);

  const deadline = Date.now() + 45_000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) throw new Error(`API exited on port ${port}:\n${logs}`);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health/live`, { signal: AbortSignal.timeout(1000) });
      if (response.ok) return;
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`API did not become ready on port ${port}:\n${logs}`);
}

async function request(instance, path, options = {}) {
  const response = await fetch(`${urls[instance % urls.length]}${path}`, {
    ...options,
    headers: { 'content-type': 'application/json', ...(options.headers ?? {}) },
    signal: AbortSignal.timeout(10_000),
  });
  const body = await response.json();
  return { statusCode: response.status, body };
}

async function createWallet(instance = 0, amount = '100.00') {
  const result = await request(instance, '/wallets', {
    method: 'POST',
    body: JSON.stringify({ playerId: randomUUID(), initialBalance: { amount, currency: 'BRL' } }),
  });
  expect(result.statusCode).toBe(201);
  return result.body;
}

function wager(wallet, { externalId, amount, kind = 'BET', reference } = {}) {
  const payload = {
    providerId: 'integration-provider',
    externalTransactionId: externalId,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: `round-${wallet.id}`,
    gameId: 'integration-test',
    kind,
    money: { amount, currency: 'BRL' },
  };
  if (reference) payload.referenceExternalTransactionId = reference;
  return payload;
}

async function submit(instance, key, payload) {
  return request(instance, '/wagering/transactions', {
    method: 'POST',
    headers: { 'Idempotency-Key': key },
    body: JSON.stringify(payload),
  });
}

beforeAll(async () => {
  if (!process.env.TEST_DATABASE_URL) throw new Error('Set TEST_DATABASE_URL to a disposable PostgreSQL database before running integration tests.');
  process.env.DATABASE_URL = process.env.TEST_DATABASE_URL;
  const config = (await import('../../dist/mikro-orm.config.js')).default;
  const orm = await MikroORM.init(config);
  try { await orm.getMigrator().up(); }
  finally { await orm.close(true); }
  for (const port of ports) await startServer(port);
}, 60_000);

afterAll(async () => {
  for (const child of children) {
    if (child.exitCode === null) child.kill('SIGTERM');
  }
  await Promise.all(children.map(async (child) => {
    if (child.exitCode !== null) return;
    await Promise.race([once(child, 'exit'), new Promise((resolve) => setTimeout(resolve, 5000))]);
    if (child.exitCode === null) child.kill('SIGKILL');
  }));
});

describe('Integração PostgreSQL e concorrência entre processos', () => {
  test('50 envios simultâneos em 3 processos geram um único débito', async () => {
    const wallet = await createWallet();
    const payload = wager(wallet, { externalId: randomUUID(), amount: '25.00' });
    const key = `integration:${payload.externalTransactionId}`;
    const results = await Promise.all(Array.from({ length: 50 }, (_, i) => submit(i, key, payload)));

    expect(results.every((result) => result.statusCode === 200)).toBe(true);
    expect(new Set(results.map((result) => result.body.transactionId)).size).toBe(1);
    expect(results.filter((result) => result.body.idempotentReplay === false)).toHaveLength(1);
    expect(results.filter((result) => result.body.idempotentReplay === true)).toHaveLength(49);

    const balance = await request(2, `/wallets/${wallet.id}`);
    expect(balance.body.balance.amount).toBe('75.00');
    const ledger = await request(1, `/wallets/${wallet.id}/ledger?limit=100`);
    expect(ledger.body.items.filter((entry) => entry.direction === 'DEBIT')).toHaveLength(1);
    const reconciliation = await request(0, `/wallets/${wallet.id}/reconciliation`, { method: 'POST', body: '{}' });
    expect(reconciliation.body.consistent).toBe(true);
    expect(reconciliation.body.difference.amount).toBe('0.00');
  }, 30_000);

  test('duas apostas simultâneas de 80.00 contra saldo 100.00 não deixam saldo negativo', async () => {
    const wallet = await createWallet();
    const first = wager(wallet, { externalId: randomUUID(), amount: '80.00' });
    const second = wager(wallet, { externalId: randomUUID(), amount: '80.00' });
    const results = await Promise.all([
      submit(0, `integration:${first.externalTransactionId}`, first),
      submit(1, `integration:${second.externalTransactionId}`, second),
    ]);

    expect(results.map((result) => result.statusCode).sort()).toEqual([200, 422]);
    expect(results.filter((result) => result.body.status === 'PROCESSED')).toHaveLength(1);
    expect(results.filter((result) => result.body.status === 'REJECTED' && result.body.failureCode === 'INSUFFICIENT_FUNDS')).toHaveLength(1);
    const balance = await request(2, `/wallets/${wallet.id}`);
    expect(balance.body.balance.amount).toBe('20.00');
    const ledger = await request(0, `/wallets/${wallet.id}/ledger?limit=100`);
    expect(ledger.body.items.filter((entry) => entry.direction === 'DEBIT')).toHaveLength(1);
    expect((await request(1, `/wallets/${wallet.id}/reconciliation`, { method: 'POST', body: '{}' })).body.consistent).toBe(true);
  }, 20_000);

  test('carteiras independentes e replay idempotente preservam o saldo original', async () => {
    const [left, right] = await Promise.all([createWallet(0), createWallet(1)]);
    const leftBet = wager(left, { externalId: randomUUID(), amount: '80.00' });
    const rightBet = wager(right, { externalId: randomUUID(), amount: '80.00' });
    const [leftResult, rightResult] = await Promise.all([
      submit(0, `integration:${leftBet.externalTransactionId}`, leftBet),
      submit(2, `integration:${rightBet.externalTransactionId}`, rightBet),
    ]);
    expect(leftResult.statusCode).toBe(200);
    expect(rightResult.statusCode).toBe(200);

    const original = wager(left, { externalId: randomUUID(), amount: '10.00' });
    const key = `integration:${original.externalTransactionId}`;
    const first = await submit(1, key, original);
    const conflict = await submit(2, key, { ...original, money: { amount: '11.00', currency: 'BRL' } });
    expect(conflict.statusCode).toBe(409);
    expect(conflict.body.code).toBe('IDEMPOTENCY_KEY_REUSED');
    const later = wager(left, { externalId: randomUUID(), amount: '5.00' });
    await submit(2, `integration:${later.externalTransactionId}`, later);
    const replay = await submit(0, key, original);
    expect(first.body.balance.amount).toBe('10.00');
    expect(replay.body.balance.amount).toBe(first.body.balance.amount);
    expect(replay.body.idempotentReplay).toBe(true);
    expect((await request(2, `/wallets/${left.id}`)).body.balance.amount).toBe('5.00');
  }, 20_000);

  test('LOSS com valor zero registra o resultado sem alterar saldo ou ledger', async () => {
    const wallet = await createWallet();
    const loss = wager(wallet, { externalId: randomUUID(), amount: '0.00', kind: 'LOSS' });
    const result = await submit(0, `integration:${loss.externalTransactionId}`, loss);
    expect(result.statusCode).toBe(200);
    expect(result.body.status).toBe('PROCESSED');
    expect(result.body.balance.amount).toBe('100.00');
    const ledger = await request(1, `/wallets/${wallet.id}/ledger?limit=100`);
    expect(ledger.body.items).toHaveLength(1);
    expect(ledger.body.items[0].direction).toBe('CREDIT');
    expect((await request(2, `/wallets/${wallet.id}/reconciliation`, { method: 'POST', body: '{}' })).body.consistent).toBe(true);
  }, 20_000);

  test('paginação do ledger retorna as próximas linhas sem repetir lançamentos', async () => {
    const wallet = await createWallet();
    const bet = wager(wallet, { externalId: randomUUID(), amount: '10.00' });
    await submit(0, `integration:${bet.externalTransactionId}`, bet);

    const firstPage = await request(0, `/wallets/${wallet.id}/ledger?limit=1`);
    expect(firstPage.statusCode).toBe(200);
    expect(firstPage.body.items).toHaveLength(1);
    expect(firstPage.body.nextCursor).toBeTruthy();

    const secondPage = await request(1, `/wallets/${wallet.id}/ledger?limit=1&cursor=${encodeURIComponent(firstPage.body.nextCursor)}`);
    expect(secondPage.statusCode).toBe(200);
    expect(secondPage.body.items).toHaveLength(1);
    expect(secondPage.body.items[0].id).not.toBe(firstPage.body.items[0].id);
    expect(secondPage.body.nextCursor).toBeNull();
  }, 20_000);

  test('WIN pendente é resolvido quando a aposta referenciada chega depois', async () => {
    const wallet = await createWallet();
    const externalBetId = randomUUID();
    const win = wager(wallet, { externalId: randomUUID(), amount: '15.00', kind: 'WIN', reference: externalBetId });
    const pending = await submit(0, `integration:${win.externalTransactionId}`, win);
    expect(pending.statusCode).toBe(202);
    expect(pending.body.status).toBe('PENDING_REFERENCE');

    const bet = wager(wallet, { externalId: externalBetId, amount: '5.00' });
    const processedBet = await submit(1, `integration:${bet.externalTransactionId}`, bet);
    expect(processedBet.statusCode).toBe(200);

    let resolved;
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      resolved = await request(2, `/wagering/transactions/${pending.body.transactionId}`);
      if (resolved.body.status === 'PROCESSED') break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    expect(resolved.body.status).toBe('PROCESSED');
    expect((await request(0, `/wallets/${wallet.id}/reconciliation`, { method: 'POST', body: '{}' })).body.consistent).toBe(true);
  }, 20_000);

  test('inbox confirma a mensagem SQS junto da aposta e deduplica o reenvio', async () => {
    const wallet = await createWallet();
    const aposta = wager(wallet, { externalId: randomUUID(), amount: '12.00' });
    const chaveDeIdempotencia = `integration:${aposta.externalTransactionId}`;
    const envelope = {
      messageId: randomUUID(),
      type: 'WagerTransactionRequested',
      data: { ...aposta, idempotencyKey: chaveDeIdempotencia },
    };
    const hashDoEnvelope = createHash('sha256').update(JSON.stringify(envelope)).digest('hex');
    const opcoesDaMensagem = {
      method: 'POST',
      headers: {
        'Idempotency-Key': chaveDeIdempotencia,
        'x-sqs-message-id': envelope.messageId,
        'x-sqs-payload-hash': hashDoEnvelope,
      },
      body: JSON.stringify(envelope.data),
    };

    const primeiraResposta = await request(0, '/wagering/transactions', opcoesDaMensagem);
    const reenvio = await request(1, '/wagering/transactions', opcoesDaMensagem);
    expect(primeiraResposta.statusCode).toBe(200);
    expect(reenvio.statusCode).toBe(200);
    expect(reenvio.body.status).toBe('DUPLICATE_MESSAGE');

    const ormDeVerificacao = await MikroORM.init((await import('../../dist/mikro-orm.config.js')).default);
    try {
      const [registroDaInbox] = await ormDeVerificacao.em.getConnection().execute(
        'select processed_at from inbox_message where consumer_name=? and message_id=?',
        ['wager-transactions', envelope.messageId],
      );
      expect(registroDaInbox.processed_at).not.toBeNull();
    } finally {
      await ormDeVerificacao.close(true);
    }
    const carteiraFinal = await request(2, `/wallets/${wallet.id}`);
    expect(carteiraFinal.body.balance.amount).toBe('88.00');
  }, 20_000);

  test('redelivery após falha no ACK não repete o débito financeiro', async () => {
    const endpointSqs = process.env.SQS_ENDPOINT_URL;
    if (!endpointSqs) throw new Error('Defina SQS_ENDPOINT_URL para testar redelivery com LocalStack.');
    const configuracaoSqs = {
      region: process.env.SQS_REGION ?? 'us-east-1',
      endpoint: endpointSqs,
      credentials: {
        accessKeyId: process.env.SQS_ACCESS_KEY ?? 'test',
        secretAccessKey: process.env.SQS_SECRET_KEY ?? 'test',
      },
    };
    const clienteSqsDeTeste = new SQSClient(configuracaoSqs);
    const envAnterior = {
      porta: process.env.PORT,
      filaPrincipal: process.env.SQS_TRANSACTIONS_QUEUE_URL,
      filaDeErros: process.env.SQS_TRANSACTIONS_DLQ_URL,
    };
    const sufixoDaFila = randomUUID().slice(0, 8);
    let urlDaFilaPrincipal;
    let urlDaFilaDeErros;
    let consumidor;
    try {
      const filaPrincipalCriada = await clienteSqsDeTeste.send(new CreateQueueCommand({
        QueueName: `ack-retry-${sufixoDaFila}.fifo`,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
      }));
      const filaDeErrosCriada = await clienteSqsDeTeste.send(new CreateQueueCommand({
        QueueName: `ack-retry-dlq-${sufixoDaFila}.fifo`,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
      }));
      urlDaFilaPrincipal = `${endpointSqs.replace(/\/$/, '')}${new URL(filaPrincipalCriada.QueueUrl).pathname}`;
      urlDaFilaDeErros = `${endpointSqs.replace(/\/$/, '')}${new URL(filaDeErrosCriada.QueueUrl).pathname}`;
      process.env.PORT = '3311';
      process.env.SQS_TRANSACTIONS_QUEUE_URL = urlDaFilaPrincipal;
      process.env.SQS_TRANSACTIONS_DLQ_URL = urlDaFilaDeErros;

      const carteira = await createWallet();
      const aposta = wager(carteira, { externalId: randomUUID(), amount: '12.00' });
      const envelope = {
        messageId: randomUUID(),
        type: 'WagerTransactionRequested',
        data: { ...aposta, idempotencyKey: `integration:${aposta.externalTransactionId}` },
      };
      consumidor = new ConsumidorDeTransacoes();
      await clienteSqsDeTeste.send(new SendMessageCommand({
        QueueUrl: urlDaFilaPrincipal,
        MessageBody: JSON.stringify(envelope),
        MessageGroupId: carteira.id,
        MessageDeduplicationId: envelope.messageId,
      }));

      const receberAtePrazo = async () => {
        const prazoFinal = Date.now() + 8_000;
        while (Date.now() < prazoFinal) {
          const recebidas = await clienteSqsDeTeste.send(new ReceiveMessageCommand({
            QueueUrl: urlDaFilaPrincipal,
            MaxNumberOfMessages: 1,
            WaitTimeSeconds: 1,
            VisibilityTimeout: 5,
            MessageSystemAttributeNames: ['ApproximateReceiveCount'],
          }));
          if (recebidas.Messages?.[0]) return recebidas.Messages[0];
        }
        throw new Error('Tempo limite aguardando redelivery da mensagem');
      };

      const primeiraEntrega = await receberAtePrazo();
      const enviarComandoSqs = consumidor.client.send.bind(consumidor.client);
      let falharPrimeiroAck = true;
      consumidor.client.send = (comando) => {
        if (comando instanceof DeleteMessageCommand && falharPrimeiroAck) {
          falharPrimeiroAck = false;
          return Promise.reject(new Error('falha de ACK injetada após o commit'));
        }
        return enviarComandoSqs(comando);
      };
      await consumidor.processarMensagem(primeiraEntrega);
      expect((await request(0, `/wallets/${carteira.id}`)).body.balance.amount).toBe('88.00');

      const segundaEntrega = await receberAtePrazo();
      expect(segundaEntrega.Attributes.ApproximateReceiveCount).toBe('2');
      await consumidor.processarMensagem(segundaEntrega);

      const carteiraDepoisDoRedelivery = await request(1, `/wallets/${carteira.id}`);
      expect(carteiraDepoisDoRedelivery.body.balance.amount).toBe('88.00');
      expect(await request(2, `/wallets/${carteira.id}/reconciliation`, { method: 'POST', body: '{}' }).then(({ body }) => body.consistent)).toBe(true);
    } finally {
      await consumidor?.onModuleDestroy();
      if (urlDaFilaPrincipal) await clienteSqsDeTeste.send(new DeleteQueueCommand({ QueueUrl: urlDaFilaPrincipal })).catch(() => {});
      if (urlDaFilaDeErros) await clienteSqsDeTeste.send(new DeleteQueueCommand({ QueueUrl: urlDaFilaDeErros })).catch(() => {});
      clienteSqsDeTeste.destroy();
      if (envAnterior.porta === undefined) delete process.env.PORT;
      else process.env.PORT = envAnterior.porta;
      if (envAnterior.filaPrincipal === undefined) delete process.env.SQS_TRANSACTIONS_QUEUE_URL;
      else process.env.SQS_TRANSACTIONS_QUEUE_URL = envAnterior.filaPrincipal;
      if (envAnterior.filaDeErros === undefined) delete process.env.SQS_TRANSACTIONS_DLQ_URL;
      else process.env.SQS_TRANSACTIONS_DLQ_URL = envAnterior.filaDeErros;
    }
  }, 30_000);

  test('processo encerrado após commit reinicia e confirma redelivery sem novo débito', async () => {
    const endpointSqs = process.env.SQS_ENDPOINT_URL;
    if (!endpointSqs) throw new Error('Defina SQS_ENDPOINT_URL para testar recuperação após queda do consumidor.');
    const clienteSqsDeTeste = new SQSClient({
      region: process.env.SQS_REGION ?? 'us-east-1',
      endpoint: endpointSqs,
      credentials: {
        accessKeyId: process.env.SQS_ACCESS_KEY ?? 'test',
        secretAccessKey: process.env.SQS_SECRET_KEY ?? 'test',
      },
    });
    const ambienteAnterior = {
      porta: process.env.PORT,
      filaPrincipal: process.env.SQS_TRANSACTIONS_QUEUE_URL,
      filaDeErros: process.env.SQS_TRANSACTIONS_DLQ_URL,
    };
    const sufixoDaFila = randomUUID().slice(0, 8);
    let urlDaFilaPrincipal;
    let urlDaFilaDeErros;

    try {
      const filaPrincipalCriada = await clienteSqsDeTeste.send(new CreateQueueCommand({
        QueueName: `consumer-crash-${sufixoDaFila}.fifo`,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
      }));
      const filaDeErrosCriada = await clienteSqsDeTeste.send(new CreateQueueCommand({
        QueueName: `consumer-crash-dlq-${sufixoDaFila}.fifo`,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
      }));
      urlDaFilaPrincipal = `${endpointSqs.replace(/\/$/, '')}${new URL(filaPrincipalCriada.QueueUrl).pathname}`;
      urlDaFilaDeErros = `${endpointSqs.replace(/\/$/, '')}${new URL(filaDeErrosCriada.QueueUrl).pathname}`;
      process.env.PORT = '3311';
      process.env.SQS_TRANSACTIONS_QUEUE_URL = urlDaFilaPrincipal;
      process.env.SQS_TRANSACTIONS_DLQ_URL = urlDaFilaDeErros;

      const carteira = await createWallet();
      const aposta = wager(carteira, { externalId: randomUUID(), amount: '14.00' });
      const envelope = {
        messageId: randomUUID(),
        type: 'WagerTransactionRequested',
        data: { ...aposta, idempotencyKey: `integration:${aposta.externalTransactionId}` },
      };
      await clienteSqsDeTeste.send(new SendMessageCommand({
        QueueUrl: urlDaFilaPrincipal,
        MessageBody: JSON.stringify(envelope),
        MessageGroupId: carteira.id,
        MessageDeduplicationId: envelope.messageId,
      }));

      const ambienteDoWorker = {
        ...process.env,
        PORT: '3311',
        SIMULAR_QUEDA_APOS_COMMIT: 'true',
      };
      const trabalhadorEncerrado = spawn(process.execPath, ['tests/integration/helpers/consumidor-uma-mensagem.js'], {
        cwd: process.cwd(),
        env: ambienteDoWorker,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      const resultadoDaQueda = await once(trabalhadorEncerrado, 'close');
      expect(resultadoDaQueda[1]).toBe('SIGKILL');
      expect((await request(0, `/wallets/${carteira.id}`)).body.balance.amount).toBe('86.00');

      const ambienteDoWorkerSubstituto = { ...process.env, PORT: '3311' };
      delete ambienteDoWorkerSubstituto.SIMULAR_QUEDA_APOS_COMMIT;
      const trabalhadorSubstituto = spawn(process.execPath, ['tests/integration/helpers/consumidor-uma-mensagem.js'], {
        cwd: process.cwd(),
        env: ambienteDoWorkerSubstituto,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      const resultadoDaRecuperacao = await once(trabalhadorSubstituto, 'close');
      expect(resultadoDaRecuperacao[0]).toBe(0);

      const ormDeVerificacao = await MikroORM.init((await import('../../dist/mikro-orm.config.js')).default);
      try {
        const [transacoesDaAposta] = await ormDeVerificacao.em.getConnection().execute(
          'select count(*)::int as quantidade from wager_transaction where provider_id=? and external_transaction_id=?',
          [aposta.providerId, aposta.externalTransactionId],
        );
        const [debitosDaCarteira] = await ormDeVerificacao.em.getConnection().execute(
          'select count(*)::int as quantidade from wallet_ledger_entry where wallet_id=? and direction=\'DEBIT\'',
          [carteira.id],
        );
        const [mensagemDaInbox] = await ormDeVerificacao.em.getConnection().execute(
          'select processed_at from inbox_message where consumer_name=? and message_id=?',
          ['wager-transactions', envelope.messageId],
        );
        expect(transacoesDaAposta.quantidade).toBe(1);
        expect(debitosDaCarteira.quantidade).toBe(1);
        expect(mensagemDaInbox.processed_at).not.toBeNull();
      } finally {
        await ormDeVerificacao.close(true);
      }
      expect((await request(2, `/wallets/${carteira.id}/reconciliation`, { method: 'POST', body: '{}' })).body.consistent).toBe(true);
    } finally {
      if (urlDaFilaPrincipal) await clienteSqsDeTeste.send(new DeleteQueueCommand({ QueueUrl: urlDaFilaPrincipal })).catch(() => {});
      if (urlDaFilaDeErros) await clienteSqsDeTeste.send(new DeleteQueueCommand({ QueueUrl: urlDaFilaDeErros })).catch(() => {});
      clienteSqsDeTeste.destroy();
      if (ambienteAnterior.porta === undefined) delete process.env.PORT;
      else process.env.PORT = ambienteAnterior.porta;
      if (ambienteAnterior.filaPrincipal === undefined) delete process.env.SQS_TRANSACTIONS_QUEUE_URL;
      else process.env.SQS_TRANSACTIONS_QUEUE_URL = ambienteAnterior.filaPrincipal;
      if (ambienteAnterior.filaDeErros === undefined) delete process.env.SQS_TRANSACTIONS_DLQ_URL;
      else process.env.SQS_TRANSACTIONS_DLQ_URL = ambienteAnterior.filaDeErros;
    }
  }, 30_000);
});
