import { Body, ConflictException, Controller, Get, Headers, NotFoundException, Param, Post, BadRequestException, Query, Res, Inject, Logger, ServiceUnavailableException, InternalServerErrorException } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { createHash, randomUUID } from 'node:crypto';
import { Money, MoneyProps } from './domain/money';
import { incrementMetric } from './observability/metrics';

type SqlRow = Record<string, unknown>;
interface HttpResponse { status(code: number): HttpResponse }
type TransactionKind = 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
const canonical = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(',')}}`;
  return JSON.stringify(value);
};
const hash = (value: unknown): string => createHash('sha256').update(canonical(value)).digest('hex');
const str = (v: unknown, field: string, maxLength = 191): string => {
  if (typeof v !== 'string' || !v.trim() || v.length > maxLength) throw new BadRequestException({ code: 'INVALID_PAYLOAD', field });
  return v;
};
const uuid = (value: string, field: string): string => {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) throw new BadRequestException({ code: 'INVALID_UUID', field });
  return value;
};
const onlyKeys = (value: Record<string, unknown>, allowed: string[], field: string): void => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new BadRequestException({ code: 'INVALID_PAYLOAD', field });
  const unknown = Object.keys(value).find((key) => !allowed.includes(key));
  if (unknown) throw new BadRequestException({ code: 'UNKNOWN_FIELD', field: `${field}.${unknown}` });
};
const parseMoney = (value: MoneyProps): Money => {
  try { return Money.from(value); }
  catch { throw new BadRequestException({ code: 'INVALID_MONEY' }); }
};

@Controller()
export class FinancialController {
  private readonly logger = new Logger(FinancialController.name);
  constructor(@Inject(MikroORM) private readonly orm: MikroORM) {}

  @Post('wallets')
  async createWallet(@Body() body: Record<string, unknown>): Promise<unknown> {
    onlyKeys(body, ['playerId', 'initialBalance'], 'body');
    const playerId = str(body.playerId, 'playerId');
    uuid(playerId, 'playerId');
    const initial = body.initialBalance as MoneyProps;
    if (!initial || typeof initial !== 'object') throw new BadRequestException({ code: 'INVALID_PAYLOAD', field: 'initialBalance' });
    onlyKeys(initial as unknown as Record<string, unknown>, ['amount', 'currency'], 'initialBalance');
    const opening = parseMoney(initial);
    if (opening.isNegative()) throw new BadRequestException({ code: 'INVALID_MONEY' });
    const id = randomUUID();
    try {
      await this.orm.em.transactional(async (em) => {
        await em.getConnection().execute('insert into wallet(id,player_id,currency,balance,version) values(?,?,?,?,1)', [id, playerId, opening.currency, opening.toJSON().amount]);
        if (opening.isPositive()) {
          const txId = randomUUID();
          await em.getConnection().execute(`insert into wager_transaction(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,status,result_balance,processed_at)
            values(?, 'internal', ?, ?, ?, ?, ?, 'opening', 'system', 'OPENING', ?, ?, 'PROCESSED', ?, now())`, [txId, txId, `opening:${id}`, hash({ id, opening: opening.toJSON() }), id, playerId, opening.toJSON().amount, opening.currency, opening.toJSON().amount]);
          await em.getConnection().execute(`insert into wallet_ledger_entry(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after) values(?,?,?,'CREDIT',?,?,0,?)`, [randomUUID(), id, txId, opening.toJSON().amount, opening.currency, opening.toJSON().amount]);
          await em.getConnection().execute(`insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)`, [randomUUID(), id, 'WalletBalanceChanged', JSON.stringify({ walletId: id, transactionId: txId, direction: 'CREDIT', money: opening.toJSON(), balanceBefore: { amount: '0.00', currency: opening.currency }, balanceAfter: opening.toJSON(), walletVersion: 1 }), txId, txId]);
          await em.getConnection().execute(`insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)`, [randomUUID(), id, 'WagerTransactionProcessed', JSON.stringify({ transactionId: txId, kind: 'OPENING', status: 'PROCESSED' }), txId, txId]);
        }
      });
    } catch (error) {
      if (String(error).includes('wallet_player_id_currency_key')) throw new ConflictException({ code: 'WALLET_ALREADY_EXISTS' });
      throw error;
    }
    return { id, playerId, balance: opening.toJSON(), version: 1 };
  }

  @Get('wallets/:walletId')
  async getWallet(@Param('walletId') walletId: string): Promise<unknown> {
    uuid(walletId, 'walletId');
    const rows = await this.orm.em.getConnection().execute<SqlRow[]>('select id,player_id,currency,balance::text,version from wallet where id=?', [walletId]);
    const row = rows[0];
    if (!row) throw new NotFoundException({ code: 'WALLET_NOT_FOUND' });
    return { id: row.id, playerId: row.player_id, balance: { amount: row.balance, currency: row.currency }, version: row.version };
  }

  @Get('wallets/:walletId/ledger')
  async ledger(@Param('walletId') walletId: string, @Query('cursor') cursor?: string, @Query('limit') limitRaw?: string): Promise<unknown> {
    const limit = Math.min(100, Math.max(1, Number.parseInt(limitRaw ?? '50', 10) || 50));
    let rows: SqlRow[];
    if (cursor) {
      let decoded: { at: string; id: string };
      try { decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as { at: string; id: string }; }
      catch { throw new BadRequestException({ code: 'INVALID_CURSOR' }); }
      if (!decoded || typeof decoded !== 'object' || typeof decoded.at !== 'string' || Number.isNaN(Date.parse(decoded.at)) || typeof decoded.id !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(decoded.id)) throw new BadRequestException({ code: 'INVALID_CURSOR' });
      rows = await this.orm.em.getConnection().execute<SqlRow[]>(`select id,transaction_id,direction,amount::text,currency,balance_before::text,balance_after::text,created_at from wallet_ledger_entry where wallet_id=? and (created_at,id)<(?::timestamptz,?::uuid) order by created_at desc,id desc limit ?`, [walletId,decoded.at,decoded.id,limit + 1]);
    } else {
      rows = await this.orm.em.getConnection().execute<SqlRow[]>(`select id,transaction_id,direction,amount::text,currency,balance_before::text,balance_after::text,created_at from wallet_ledger_entry where wallet_id=? order by created_at desc,id desc limit ?`, [walletId,limit + 1]);
    }
    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const last = page[page.length - 1];
    return { items: page.map((row) => ({ id: row.id, transactionId: row.transaction_id, direction: row.direction, money: { amount: row.amount, currency: row.currency }, balanceBefore: { amount: row.balance_before, currency: row.currency }, balanceAfter: { amount: row.balance_after, currency: row.currency }, createdAt: row.created_at })), nextCursor: hasMore && last ? Buffer.from(JSON.stringify({ at: new Date(String(last.created_at)).toISOString(), id: last.id })).toString('base64url') : null };
  }

  @Post('wallets/:walletId/reconciliation')
  async reconcile(@Param('walletId') walletId: string): Promise<unknown> {
    uuid(walletId, 'walletId');
    const rows = await this.orm.em.getConnection().execute<SqlRow[]>(`select w.currency,w.balance::text as stored, coalesce(sum(case when l.direction='CREDIT' then l.amount else -l.amount end),0)::numeric(19,2)::text as calculated,count(l.id)::int as entries from wallet w left join wallet_ledger_entry l on l.wallet_id=w.id where w.id=? group by w.id`, [walletId]);
    const row = rows[0];
    if (!row) throw new NotFoundException({ code: 'WALLET_NOT_FOUND' });
    const currency = String(row.currency);
    const stored = Money.from({ amount: String(row.stored), currency });
    const calculated = Money.from({ amount: String(row.calculated), currency });
    const diff = stored.isLessThan(calculated) ? calculated.subtract(stored) : stored.subtract(calculated);
    const consistent = stored.equals(calculated);
    if (!consistent) {
      incrementMetric('reconciliation_mismatches_total');
      this.logger.error(JSON.stringify({ event: 'wallet_reconciliation_mismatch', correlationId: null, messageId: null, transactionId: null, walletId }));
    }
    return { walletId, storedBalance: stored.toJSON(), calculatedBalance: calculated.toJSON(), difference: { amount: diff.isZero() ? '0.00' : diff.toJSON().amount, currency, signedAmount: stored.isLessThan(calculated) ? diff.negate().toJSON().amount : diff.toJSON().amount }, consistent, checkedEntries: row.entries };
  }

  @Get('wagering/transactions/:transactionId')
  async getTransaction(@Param('transactionId') transactionId: string): Promise<unknown> {
    uuid(transactionId, 'transactionId');
    const rows = await this.orm.em.getConnection().execute<SqlRow[]>('select id,provider_id,external_transaction_id,kind,status,failure_code,wallet_id,amount::text,currency,result_balance::text,created_at,processed_at from wager_transaction where id=?', [transactionId]);
    if (!rows[0]) throw new NotFoundException({ code: 'TRANSACTION_NOT_FOUND' });
    return rows[0];
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  async getProviderTransaction(@Param('providerId') providerId: string, @Param('externalTransactionId') externalId: string): Promise<unknown> {
    str(providerId, 'providerId', 64);
    str(externalId, 'externalTransactionId', 128);
    const rows = await this.orm.em.getConnection().execute<SqlRow[]>('select id,provider_id,external_transaction_id,kind,status,failure_code,wallet_id,amount::text,currency,result_balance::text,created_at,processed_at from wager_transaction where provider_id=? and external_transaction_id=?', [providerId,externalId]);
    if (!rows[0]) throw new NotFoundException({ code: 'TRANSACTION_NOT_FOUND' });
    return rows[0];
  }

  @Post('wagering/transactions')
  async submit(
    @Headers('idempotency-key') keyHeader: string | undefined,
    @Body() body: Record<string, unknown>,
    @Res({ passthrough: true }) response: HttpResponse,
    @Headers('x-sqs-message-id') messageId?: string,
    @Headers('x-sqs-payload-hash') messageHash?: string,
    @Headers('x-correlation-id') requestCorrelationId?: string,
  ): Promise<unknown> {
    const startedAt = Date.now();
    if (!keyHeader) throw new BadRequestException({ code: 'IDEMPOTENCY_KEY_REQUIRED' });
    onlyKeys(body, ['providerId', 'externalTransactionId', 'playerId', 'walletId', 'roundId', 'gameId', 'kind', 'money', 'referenceExternalTransactionId', 'idempotencyKey'], 'body');
    const providerId = str(body.providerId, 'providerId', 64);
    const externalId = str(body.externalTransactionId, 'externalTransactionId', 128);
    const playerId = str(body.playerId, 'playerId');
    uuid(playerId, 'playerId');
    const walletId = uuid(str(body.walletId, 'walletId'), 'walletId');
    const roundId = str(body.roundId, 'roundId', 128);
    const gameId = str(body.gameId, 'gameId', 128);
    const kind = str(body.kind, 'kind') as TransactionKind;
    if (!['BET','WIN','LOSS','REFUND','ROLLBACK'].includes(kind)) throw new BadRequestException({ code: 'INVALID_KIND' });
    const moneyProps = body.money as MoneyProps;
    if (!moneyProps || typeof moneyProps !== 'object') throw new BadRequestException({ code: 'INVALID_MONEY' });
    onlyKeys(moneyProps as unknown as Record<string, unknown>, ['amount', 'currency'], 'money');
    const money = parseMoney(moneyProps);
    if (kind === 'LOSS' ? !money.isZero() : !money.isPositive()) {
      throw new BadRequestException({ code: kind === 'LOSS' ? 'LOSS_AMOUNT_MUST_BE_ZERO' : 'AMOUNT_MUST_BE_POSITIVE' });
    }
    const referenceExternalId = body.referenceExternalTransactionId === undefined ? undefined : str(body.referenceExternalTransactionId, 'referenceExternalTransactionId', 128);
    if ((kind === 'REFUND' || kind === 'ROLLBACK') !== Boolean(referenceExternalId)) throw new BadRequestException({ code: 'REFERENCE_REQUIRED' });
    const idempotencyKey = str(keyHeader, 'Idempotency-Key', 191);
    if (body.idempotencyKey !== undefined && body.idempotencyKey !== idempotencyKey) throw new BadRequestException({ code: 'IDEMPOTENCY_KEY_MISMATCH' });
    const payloadHash = hash({ providerId, externalId, playerId, walletId, roundId, gameId, kind, money: money.toJSON(), referenceExternalId });

    let outcome: unknown;
    try {
    outcome = await this.orm.em.transactional(async (em) => {
      const conn = em.getConnection();
      if (messageId || messageHash) {
        if (!messageId || !messageHash || !/^[a-f0-9]{64}$/.test(messageHash)) throw new BadRequestException({ code: 'INVALID_INBOX_METADATA' });
        const received = await conn.execute<SqlRow[]>(`insert into inbox_message(consumer_name,message_id,payload_hash)
          values('wager-transactions',?,?) on conflict do nothing returning message_id`, [messageId, messageHash]);
        if (!received.length) {
          const existing = await conn.execute<SqlRow[]>('select payload_hash from inbox_message where consumer_name=? and message_id=?', ['wager-transactions', messageId]);
          if (existing[0]?.payload_hash !== messageHash) throw new ConflictException({ code: 'SQS_MESSAGE_ID_REUSED' });
          incrementMetric('sqs_inbox_duplicates_total');
          return { status: 'DUPLICATE_MESSAGE', idempotentReplay: true };
        }
      }
      const priorRows = await conn.execute<SqlRow[]>('select id,payload_hash,status,failure_code,result_balance::text,currency from wager_transaction where provider_id=? and idempotency_key=? for update', [providerId,idempotencyKey]);
      if (priorRows[0]) {
        const prior = priorRows[0];
        if (prior.payload_hash !== payloadHash) throw new ConflictException({ code: 'IDEMPOTENCY_KEY_REUSED' });
        incrementMetric('idempotent_replays_total');
        if (messageId) await conn.execute('update inbox_message set processed_at=now() where consumer_name=? and message_id=?', ['wager-transactions', messageId]);
        return { transactionId: prior.id, status: prior.status, failureCode: prior.failure_code, balance: { amount: prior.result_balance ?? '0.00', currency: prior.currency }, idempotentReplay: true };
      }

      const txId = randomUUID();
      const walletRows = await conn.execute<SqlRow[]>('select id,player_id,currency,balance::text,version from wallet where id=? for update', [walletId]);
      const wallet = walletRows[0];
      if (!wallet) throw new NotFoundException({ code: 'WALLET_NOT_FOUND' });
      if (wallet.player_id !== playerId) throw new ConflictException({ code: 'PLAYER_WALLET_MISMATCH' });
      const balance = Money.from({ amount: String(wallet.balance), currency: String(wallet.currency) });
      if (money.currency !== balance.currency) throw new ConflictException({ code: 'CURRENCY_MISMATCH' });
      let status = 'PROCESSED';
      let failureCode: string | null = null;
      let after = balance;
      let direction: 'DEBIT' | 'CREDIT' | undefined;
      let referenceId: string | null = null;

      let reference: SqlRow | undefined;
      if (referenceExternalId) {
        const refs = await conn.execute<SqlRow[]>(`select id,kind,status,amount::text,currency,player_id,wallet_id,round_id from wager_transaction where provider_id=? and external_transaction_id=? for update`, [providerId, referenceExternalId]);
        reference = refs[0];
        if (!reference) status = 'PENDING_REFERENCE';
        else if (reference.status === 'PENDING' || reference.status === 'PENDING_REFERENCE') status = 'PENDING_REFERENCE';
        else {
          const reversal = kind === 'REFUND' || kind === 'ROLLBACK';
          const sameContext = reference.player_id === playerId && reference.wallet_id === walletId && reference.round_id === roundId && reference.currency === money.currency;
          if (!sameContext || reversal && String(reference.amount) !== money.toJSON().amount) { status='REJECTED'; failureCode='INVALID_REFERENCE'; }
          else {
            referenceId = String(reference.id);
            if (reference.status !== 'PROCESSED') { status='REJECTED'; failureCode='INVALID_REFERENCE'; }
            else if (kind === 'REFUND' && reference.kind !== 'BET' || kind === 'ROLLBACK' && !['BET','WIN','REFUND'].includes(String(reference.kind)) || kind === 'WIN' && reference.kind !== 'BET') { status='REJECTED'; failureCode='INVALID_REFERENCE_KIND'; }
          }
          if (status === 'PROCESSED') {
            if (reversal) {
              const duplicates = await conn.execute<SqlRow[]>('select id from wager_transaction where reference_external_transaction_id=? and provider_id=? and kind=? and status in (\'PROCESSED\',\'PENDING_REFERENCE\') limit 1', [referenceExternalId,providerId,kind]);
              if (duplicates.length) { status='REJECTED'; failureCode='REFERENCE_ALREADY_REVERSED'; }
            }
            if (status === 'PROCESSED') direction = kind === 'WIN' || kind === 'REFUND' || reference.kind === 'BET' ? 'CREDIT' : 'DEBIT';
          }
        }
      } else if (kind === 'BET') direction = 'DEBIT';
      else if (kind === 'WIN') direction = 'CREDIT';

      if (status === 'PROCESSED' && direction === 'DEBIT') {
        try { after = balance.subtract(money); } catch { status='REJECTED'; failureCode = referenceId ? 'REVERSAL_WOULD_OVERDRAW' : 'INSUFFICIENT_FUNDS'; }
      } else if (status === 'PROCESSED' && direction === 'CREDIT') after = balance.add(money);

      await conn.execute(`insert into wager_transaction(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,reference_external_transaction_id,reference_transaction_id,status,failure_code,result_balance,processed_at,reference_next_attempt_at,reference_expires_at)
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,case when ? in ('PROCESSED','REJECTED') then now() else null end,
          case when ?='PENDING_REFERENCE' then now()+interval '2 seconds' else null end,
          case when ?='PENDING_REFERENCE' then now()+interval '24 hours' else null end)`, [txId,providerId,externalId,idempotencyKey,payloadHash,walletId,playerId,roundId,gameId,kind,money.toJSON().amount,money.currency,referenceExternalId ?? null,referenceId,status,failureCode,after.toJSON().amount,status,status,status]);

      if (status === 'PROCESSED' && direction) {
        await conn.execute('update wallet set balance=?,version=version+1,updated_at=now() where id=?', [after.toJSON().amount,walletId]);
        await conn.execute('insert into wallet_ledger_entry(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after) values(?,?,?,?,?,?,?,?)', [randomUUID(),walletId,txId,direction,money.toJSON().amount,money.currency,balance.toJSON().amount,after.toJSON().amount]);
        await conn.execute('insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)', [randomUUID(),walletId,'WalletBalanceChanged',JSON.stringify({ walletId, transactionId: txId, direction, money: money.toJSON(), balanceBefore: balance.toJSON(), balanceAfter: after.toJSON(), walletVersion: Number(wallet.version) + 1 }),requestCorrelationId ?? txId,messageId ?? txId]);
      }
      if (status === 'PROCESSED' || status === 'REJECTED' || status === 'PENDING_REFERENCE') {
        const eventType = status === 'PROCESSED' ? 'WagerTransactionProcessed' : status === 'REJECTED' ? 'WagerTransactionRejected' : 'WagerTransactionPendingReference';
        await conn.execute('insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)', [randomUUID(),walletId,eventType,JSON.stringify({ transactionId: txId, providerId, externalTransactionId: externalId, walletId, kind, status, failureCode, referenceExternalTransactionId: referenceExternalId ?? null }),requestCorrelationId ?? txId,messageId ?? txId]);
      }
      if (messageId) await conn.execute('update inbox_message set processed_at=now() where consumer_name=? and message_id=?', ['wager-transactions', messageId]);
      return { transactionId: txId, status, failureCode, balance: after.toJSON(), idempotentReplay: false };
    });
    } catch (error) {
      const dbError = error as { code?: string; cause?: { code?: string } };
      const code = dbError.code ?? dbError.cause?.code;
      if (code === '40P01' || code === '55P03') incrementMetric('database_lock_conflicts_total');
      if (['08000','08003','08006','53300','57P01','40P01','55P03'].includes(String(code))) throw new ServiceUnavailableException({ code: 'TRANSIENT_DATABASE_FAILURE' });
      if (['22P02','22003'].includes(String(code))) throw new BadRequestException({ code: 'INVALID_PAYLOAD' });
      if (['23503','23514'].includes(String(code))) throw new InternalServerErrorException({ code: 'FINANCIAL_CONSTRAINT_VIOLATION' });
      if (code !== '23505') throw error;
      const rows = await this.orm.em.getConnection().execute<SqlRow[]>('select id,idempotency_key,payload_hash,status,failure_code,result_balance::text,currency from wager_transaction where (provider_id=? and idempotency_key=?) or (provider_id=? and external_transaction_id=?)', [providerId,idempotencyKey,providerId,externalId]);
      const prior = rows[0];
      if (!prior) throw error;
      if (prior.idempotency_key !== idempotencyKey) throw new ConflictException({ code: 'EXTERNAL_TRANSACTION_ID_REUSED' });
      if (prior.payload_hash !== payloadHash) throw new ConflictException({ code: 'IDEMPOTENCY_KEY_REUSED' });
      outcome = { transactionId: prior.id, status: prior.status, failureCode: prior.failure_code, balance: { amount: prior.result_balance ?? '0.00', currency: prior.currency }, idempotentReplay: true };
    }
    const result = outcome as { status: string; transactionId?: string };
    incrementMetric('wager_processing_count_total');
    incrementMetric('wager_processing_duration_ms_total', Date.now() - startedAt);
    this.logger.log(JSON.stringify({ event: 'wager_processing_finished', correlationId: requestCorrelationId ?? result.transactionId ?? null, messageId: messageId ?? null, transactionId: result.transactionId ?? null, walletId, providerId, status: result.status }));
    response.status(result.status === 'PENDING_REFERENCE' || result.status === 'PENDING' ? 202 : result.status === 'REJECTED' ? 422 : 200);
    return outcome;
  }
}
