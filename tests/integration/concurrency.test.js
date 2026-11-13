import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { MikroORM } from '@mikro-orm/postgresql';

const ports = [3311, 3312, 3313];
const children = [];
const urls = ports.map((port) => `http://127.0.0.1:${port}`);

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

describe('PostgreSQL integration and multi-process concurrency', () => {
  test('50 parallel copies across 3 app processes create exactly one debit', async () => {
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

  test('two concurrent 80.00 bets against 100.00 yield one debit and no negative balance', async () => {
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

  test('wallets are processed independently and idempotent replay returns its original balance snapshot', async () => {
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

  test('LOSS with zero amount records an outcome without changing balance or ledger', async () => {
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
});
