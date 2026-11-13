import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { randomUUID } from 'node:crypto';
import { Money } from '../domain/money';
import { incrementMetric } from '../observability/metrics';
import { resumirErroParaLog } from '../observability/error-summary';

type Row = Record<string, unknown>;

@Injectable()
export class AgendadorDeReferencias implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(AgendadorDeReferencias.name);
  private timer?: NodeJS.Timeout;
  private running?: Promise<void>;
  private stopping = false;

  constructor(@Inject(MikroORM) private readonly orm: MikroORM) {}

  onModuleInit(): void {
    this.timer = setInterval(() => {
      if (!this.running && !this.stopping) this.processarLoteDeReferencias().finally(() => { this.running = undefined; });
    }, 1000);
    this.running = this.processarLoteDeReferencias().finally(() => { this.running = undefined; });
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    if (this.timer) clearInterval(this.timer);
    await this.running;
  }

  /** Reserva uma referência pendente por transação para permitir múltiplos workers. */
  private async processarLoteDeReferencias(): Promise<void> {
    for (let i = 0; i < 25 && !this.stopping; i++) {
      try {
        const worked = await this.orm.em.transactional(async (gerenteTransacional) => {

          const linhasPendentes = await gerenteTransacional.execute<Row[]>(`select * from wager_transaction
            where status='PENDING_REFERENCE' and (reference_next_attempt_at is null or reference_next_attempt_at<=now())
            order by reference_next_attempt_at nulls first,created_at for update skip locked limit 1`);
          if (!linhasPendentes[0]) return false;
          await this.processarUmaReferencia(gerenteTransacional, linhasPendentes[0]);
          return true;
        });
        if (!worked) return;
      } catch (error) {
        this.logger.error(JSON.stringify({ event: 'pending_reference_retry_failed', correlationId: null, messageId: null, transactionId: null, ...resumirErroParaLog(error) }));
        return;
      }
    }
  }

  /** Revalida o contexto financeiro e aplica saldo, ledger e outbox atomicamente. */
  private async processarUmaReferencia(gerenteTransacional: MikroORM['em'], transacaoPendente: Row): Promise<void> {
    const transactionId = String(transacaoPendente.id);
    const now = Date.now();
    const expired = transacaoPendente.reference_expires_at != null && new Date(String(transacaoPendente.reference_expires_at)).getTime() <= now;
    const attempts = Number(transacaoPendente.reference_attempts ?? 0);
    const maxAttempts = 10;
    const contexto = { providerId: String(transacaoPendente.provider_id), externalId: String(transacaoPendente.external_transaction_id), kind: String(transacaoPendente.kind), walletId: String(transacaoPendente.wallet_id), playerId: String(transacaoPendente.player_id), roundId: String(transacaoPendente.round_id), currency: String(transacaoPendente.currency), amount: String(transacaoPendente.amount), referenceExternalId: String(transacaoPendente.reference_external_transaction_id) };

    if (expired || attempts >= maxAttempts) {
      await this.rejeitarTransacaoPendente(gerenteTransacional, transacaoPendente, 'REFERENCE_NOT_FOUND', null);
      return;
    }

    const references = await gerenteTransacional.execute<Row[]>(`select id,kind,status,amount::text,player_id,wallet_id,round_id,currency
      from wager_transaction where provider_id=? and external_transaction_id=?`, [contexto.providerId, contexto.referenceExternalId]);
    const reference = references[0];
    if (!reference) {
      await this.agendarNovaTentativa(gerenteTransacional, transactionId, attempts);
      return;
    }
    if (reference.status === 'PENDING' || reference.status === 'PENDING_REFERENCE') {
      await this.agendarNovaTentativa(gerenteTransacional, transactionId, attempts);
      return;
    }

    const reversal = contexto.kind === 'REFUND' || contexto.kind === 'ROLLBACK';
    let failureCode: string | null = null;
    const sameContext = reference.player_id === contexto.playerId && reference.wallet_id === contexto.walletId && reference.round_id === contexto.roundId && reference.currency === contexto.currency;
    if (!sameContext || reference.status !== 'PROCESSED' || reversal && String(reference.amount) !== contexto.amount) failureCode = 'INVALID_REFERENCE';
    else if (contexto.kind === 'REFUND' && reference.kind !== 'BET' || contexto.kind === 'ROLLBACK' && !['BET', 'WIN', 'REFUND'].includes(String(reference.kind)) || contexto.kind === 'WIN' && reference.kind !== 'BET') failureCode = 'INVALID_REFERENCE_KIND';
    if (reversal && !failureCode) {
      const duplicates = await gerenteTransacional.execute<Row[]>('select id from wager_transaction where provider_id=? and reference_external_transaction_id=? and kind=? and status in (\'PROCESSED\',\'PENDING_REFERENCE\') and id<>? limit 1', [contexto.providerId, contexto.referenceExternalId, contexto.kind, transactionId]);
      if (duplicates.length) failureCode = 'REFERENCE_ALREADY_REVERSED';
    }
    if (failureCode) {
      await this.rejeitarTransacaoPendente(gerenteTransacional, transacaoPendente, failureCode, sameContext ? String(reference.id) : null);
      return;
    }

    const wallets = await gerenteTransacional.execute<Row[]>('select balance::text,currency,version from wallet where id=? for update', [contexto.walletId]);
    const wallet = wallets[0];
    if (!wallet) {
      await this.rejeitarTransacaoPendente(gerenteTransacional, transacaoPendente, 'WALLET_NOT_FOUND', String(reference.id));
      return;
    }
    const before = Money.from({ amount: String(wallet.balance), currency: String(wallet.currency) });
    const amount = Money.from({ amount: contexto.amount, currency: contexto.currency });
    const direction: 'CREDIT' | 'DEBIT' = contexto.kind === 'WIN' || contexto.kind === 'REFUND' || reference.kind === 'BET' ? 'CREDIT' : 'DEBIT';
    let after = before;
    if (direction === 'DEBIT') {
      try { after = before.subtract(amount); }
      catch { await this.rejeitarTransacaoPendente(gerenteTransacional, transacaoPendente, 'REVERSAL_WOULD_OVERDRAW', String(reference.id)); return; }
    } else after = before.add(amount);

    await gerenteTransacional.execute(`update wager_transaction set status='PROCESSED',failure_code=null,result_balance=?,processed_at=now(),reference_transaction_id=?,reference_next_attempt_at=null where id=?`, [after.toJSON().amount, reference.id, transactionId]);
    await gerenteTransacional.execute('update wallet set balance=?,version=version+1,updated_at=now() where id=?', [after.toJSON().amount, contexto.walletId]);
    await gerenteTransacional.execute('insert into wallet_ledger_entry(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after) values(?,?,?,?,?,?,?,?)', [randomUUID(), contexto.walletId, transactionId, direction, amount.toJSON().amount, contexto.currency, before.toJSON().amount, after.toJSON().amount]);
    await gerenteTransacional.execute('insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)', [randomUUID(), contexto.walletId, 'WalletBalanceChanged', JSON.stringify({ transactionId, direction, money: amount.toJSON(), balanceBefore: before.toJSON(), balanceAfter: after.toJSON(), walletVersion: Number(wallet.version) + 1 }), transactionId, String(reference.id)]);
    await gerenteTransacional.execute('insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)', [randomUUID(), contexto.walletId, 'WagerTransactionProcessed', JSON.stringify({ transactionId, providerId: contexto.providerId, externalTransactionId: contexto.externalId, kind: contexto.kind, status: 'PROCESSED', referenceTransactionId: reference.id }), transactionId, String(reference.id)]);
    this.logger.log(JSON.stringify({ event: 'pending_reference_resolved', correlationId: transactionId, messageId: null, transactionId, walletId: contexto.walletId, providerId: contexto.providerId }));
  }

  private async agendarNovaTentativa(gerenteTransacional: MikroORM['em'], transactionId: string, attempts: number): Promise<void> {
    const nextAttempt = attempts + 1;
    const delay = Math.min(300, 2 ** nextAttempt);
    incrementMetric('pending_reference_retries_total');
    await gerenteTransacional.execute(`update wager_transaction set reference_attempts=?,reference_next_attempt_at=now()+(? * interval '1 second') where id=?`, [nextAttempt, delay, transactionId]);
  }

  private async rejeitarTransacaoPendente(gerenteTransacional: MikroORM['em'], transacaoPendente: Row, failureCode: string, referenceId: string | null): Promise<void> {
    const transactionId = String(transacaoPendente.id);
    await gerenteTransacional.execute(`update wager_transaction set status='REJECTED',failure_code=?,result_balance=(select balance::text from wallet where id=wager_transaction.wallet_id),processed_at=now(),reference_transaction_id=?,reference_next_attempt_at=null where id=?`, [failureCode, referenceId, transactionId]);
    await gerenteTransacional.execute('insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)', [randomUUID(), transacaoPendente.wallet_id, 'WagerTransactionRejected', JSON.stringify({ transactionId, providerId: transacaoPendente.provider_id, externalTransactionId: transacaoPendente.external_transaction_id, kind: transacaoPendente.kind, status: 'REJECTED', failureCode }), transactionId, referenceId ?? transactionId]);
    this.logger.warn(JSON.stringify({ event: 'pending_reference_rejected', correlationId: transactionId, messageId: null, transactionId, walletId: transacaoPendente.wallet_id, providerId: transacaoPendente.provider_id, failureCode }));
  }
}
