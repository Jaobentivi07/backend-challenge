import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { randomUUID } from 'node:crypto';
import { Money } from '../domain/money';
import { incrementMetric } from '../observability/metrics';

type Row = Record<string, unknown>;

@Injectable()
export class ReferenceScheduler implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ReferenceScheduler.name);
  private timer?: NodeJS.Timeout;
  private running?: Promise<void>;
  private stopping = false;

  constructor(@Inject(MikroORM) private readonly orm: MikroORM) {}

  onModuleInit(): void {
    this.timer = setInterval(() => {
      if (!this.running && !this.stopping) this.running = this.runBatch().finally(() => { this.running = undefined; });
    }, 1000);
    this.running = this.runBatch().finally(() => { this.running = undefined; });
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    if (this.timer) clearInterval(this.timer);
    await this.running;
  }

  private async runBatch(): Promise<void> {
    for (let i = 0; i < 25 && !this.stopping; i++) {
      try {
        const worked = await this.orm.em.transactional(async (em) => {
          const conn = em.getConnection();
          const rows = await conn.execute<Row[]>(`select * from wager_transaction
            where status='PENDING_REFERENCE' and (reference_next_attempt_at is null or reference_next_attempt_at<=now())
            order by reference_next_attempt_at nulls first,created_at for update skip locked limit 1`);
          if (!rows[0]) return false;
          await this.processOne(conn, rows[0]);
          return true;
        });
        if (!worked) return;
      } catch (error) {
        this.logger.error(JSON.stringify({ event: 'pending_reference_retry_failed', correlationId: null, messageId: null, transactionId: null, error: error instanceof Error ? error.message : 'unknown' }));
        return;
      }
    }
  }

  private async processOne(conn: ReturnType<MikroORM['em']['getConnection']>, pending: Row): Promise<void> {
    const transactionId = String(pending.id);
    const now = Date.now();
    const expired = pending.reference_expires_at != null && new Date(String(pending.reference_expires_at)).getTime() <= now;
    const attempts = Number(pending.reference_attempts ?? 0);
    const maxAttempts = 10;
    const context = { providerId: String(pending.provider_id), externalId: String(pending.external_transaction_id), kind: String(pending.kind), walletId: String(pending.wallet_id), playerId: String(pending.player_id), roundId: String(pending.round_id), currency: String(pending.currency), amount: String(pending.amount), referenceExternalId: String(pending.reference_external_transaction_id) };

    if (expired || attempts >= maxAttempts) {
      await this.reject(conn, pending, 'REFERENCE_NOT_FOUND', null);
      return;
    }

    const references = await conn.execute<Row[]>(`select id,kind,status,amount::text,player_id,wallet_id,round_id,currency
      from wager_transaction where provider_id=? and external_transaction_id=?`, [context.providerId, context.referenceExternalId]);
    const reference = references[0];
    if (!reference) {
      await this.scheduleRetry(conn, transactionId, attempts);
      return;
    }
    if (reference.status === 'PENDING' || reference.status === 'PENDING_REFERENCE') {
      await this.scheduleRetry(conn, transactionId, attempts);
      return;
    }

    const reversal = context.kind === 'REFUND' || context.kind === 'ROLLBACK';
    let failureCode: string | null = null;
    const sameContext = reference.player_id === context.playerId && reference.wallet_id === context.walletId && reference.round_id === context.roundId && reference.currency === context.currency;
    if (!sameContext || reference.status !== 'PROCESSED' || reversal && String(reference.amount) !== context.amount) failureCode = 'INVALID_REFERENCE';
    else if (context.kind === 'REFUND' && reference.kind !== 'BET' || context.kind === 'ROLLBACK' && !['BET', 'WIN', 'REFUND'].includes(String(reference.kind)) || context.kind === 'WIN' && reference.kind !== 'BET') failureCode = 'INVALID_REFERENCE_KIND';
    if (reversal && !failureCode) {
      const duplicates = await conn.execute<Row[]>('select id from wager_transaction where provider_id=? and reference_external_transaction_id=? and kind=? and status in (\'PROCESSED\',\'PENDING_REFERENCE\') and id<>? limit 1', [context.providerId, context.referenceExternalId, context.kind, transactionId]);
      if (duplicates.length) failureCode = 'REFERENCE_ALREADY_REVERSED';
    }
    if (failureCode) {
      await this.reject(conn, pending, failureCode, sameContext ? String(reference.id) : null);
      return;
    }

    const wallets = await conn.execute<Row[]>('select balance::text,currency,version from wallet where id=? for update', [context.walletId]);
    const wallet = wallets[0];
    if (!wallet) {
      await this.reject(conn, pending, 'WALLET_NOT_FOUND', String(reference.id));
      return;
    }
    const before = Money.from({ amount: String(wallet.balance), currency: String(wallet.currency) });
    const amount = Money.from({ amount: context.amount, currency: context.currency });
    const direction: 'CREDIT' | 'DEBIT' = context.kind === 'WIN' || context.kind === 'REFUND' || reference.kind === 'BET' ? 'CREDIT' : 'DEBIT';
    let after = before;
    if (direction === 'DEBIT') {
      try { after = before.subtract(amount); }
      catch { await this.reject(conn, pending, 'REVERSAL_WOULD_OVERDRAW', String(reference.id)); return; }
    } else after = before.add(amount);

    await conn.execute(`update wager_transaction set status='PROCESSED',failure_code=null,result_balance=?,processed_at=now(),reference_transaction_id=?,reference_next_attempt_at=null where id=?`, [after.toJSON().amount, reference.id, transactionId]);
    await conn.execute('update wallet set balance=?,version=version+1,updated_at=now() where id=?', [after.toJSON().amount, context.walletId]);
    await conn.execute('insert into wallet_ledger_entry(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after) values(?,?,?,?,?,?,?,?)', [randomUUID(), context.walletId, transactionId, direction, amount.toJSON().amount, context.currency, before.toJSON().amount, after.toJSON().amount]);
    await conn.execute('insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)', [randomUUID(), context.walletId, 'WalletBalanceChanged', JSON.stringify({ transactionId, direction, money: amount.toJSON(), balanceBefore: before.toJSON(), balanceAfter: after.toJSON(), walletVersion: Number(wallet.version) + 1 }), transactionId, String(reference.id)]);
    await conn.execute('insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)', [randomUUID(), context.walletId, 'WagerTransactionProcessed', JSON.stringify({ transactionId, providerId: context.providerId, externalTransactionId: context.externalId, kind: context.kind, status: 'PROCESSED', referenceTransactionId: reference.id }), transactionId, String(reference.id)]);
    this.logger.log(JSON.stringify({ event: 'pending_reference_resolved', correlationId: transactionId, messageId: null, transactionId, walletId: context.walletId, providerId: context.providerId }));
  }

  private async scheduleRetry(conn: ReturnType<MikroORM['em']['getConnection']>, transactionId: string, attempts: number): Promise<void> {
    const nextAttempt = attempts + 1;
    const delay = Math.min(300, 2 ** nextAttempt);
    incrementMetric('pending_reference_retries_total');
    await conn.execute(`update wager_transaction set reference_attempts=?,reference_next_attempt_at=now()+(? * interval '1 second') where id=?`, [nextAttempt, delay, transactionId]);
  }

  private async reject(conn: ReturnType<MikroORM['em']['getConnection']>, pending: Row, failureCode: string, referenceId: string | null): Promise<void> {
    const transactionId = String(pending.id);
    await conn.execute(`update wager_transaction set status='REJECTED',failure_code=?,result_balance=(select balance::text from wallet where id=wager_transaction.wallet_id),processed_at=now(),reference_transaction_id=?,reference_next_attempt_at=null where id=?`, [failureCode, referenceId, transactionId]);
    await conn.execute('insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)', [randomUUID(), pending.wallet_id, 'WagerTransactionRejected', JSON.stringify({ transactionId, providerId: pending.provider_id, externalTransactionId: pending.external_transaction_id, kind: pending.kind, status: 'REJECTED', failureCode }), transactionId, referenceId ?? transactionId]);
    this.logger.warn(JSON.stringify({ event: 'pending_reference_rejected', correlationId: transactionId, messageId: null, transactionId, walletId: pending.wallet_id, providerId: pending.provider_id, failureCode }));
  }
}
