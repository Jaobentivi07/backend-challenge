import {
  BadRequestException,
  ConflictException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
  ServiceUnavailableException,
  Inject,
} from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { randomUUID } from 'node:crypto';
import { Money } from '../domain/money';
import { incrementMetric } from '../observability/metrics';

type LinhaDoBanco = Record<string, unknown>;
type TipoDeTransacao = 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';

export interface DadosDaTransacaoFinanceira {
  providerId: string;
  externalId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: TipoDeTransacao;
  amount: Money;
  referenceExternalId?: string;
  idempotencyKey: string;
  payloadHash: string;
  messageId?: string;
  messageHash?: string;
  correlationId?: string;
}

/** Coordena a transação, a carteira, o ledger, a inbox e a outbox sem mudar suas regras. */
@Injectable()
export class ProcessadorDeTransacoesFinanceiras {
  constructor(@Inject(MikroORM) private readonly orm: MikroORM) {}

  /** Executa toda a gravação financeira em um único commit do PostgreSQL. */
  async processarTransacao(dados: DadosDaTransacaoFinanceira): Promise<unknown> {
    const {
      providerId,
      externalId,
      playerId,
      walletId,
      roundId,
      gameId,
      kind,
      amount: money,
      referenceExternalId,
      idempotencyKey,
      payloadHash,
      messageId,
      messageHash,
      correlationId: requestCorrelationId,
    } = dados;
    let resultado: unknown;

    try {
      resultado = await this.orm.em.transactional(async (gerenteTransacional) => {
        // Todas as escritas usam o gerenciador da transação atual.
        if (messageId || messageHash) {
          if (!messageId || !messageHash || !/^[a-f0-9]{64}$/.test(messageHash)) {
            throw new BadRequestException({ code: 'INVALID_INBOX_METADATA' });
          }
          const mensagemRecebida = await gerenteTransacional.execute<LinhaDoBanco[]>(`insert into inbox_message(consumer_name,message_id,payload_hash)
            values('wager-transactions',?,?) on conflict do nothing returning message_id`, [messageId, messageHash]);
          if (!mensagemRecebida.length) {
            const mensagemExistente = await gerenteTransacional.execute<LinhaDoBanco[]>(
              'select payload_hash from inbox_message where consumer_name=? and message_id=?',
              ['wager-transactions', messageId],
            );
            if (mensagemExistente[0]?.payload_hash !== messageHash) {
              throw new ConflictException({ code: 'SQS_MESSAGE_ID_REUSED' });
            }
            incrementMetric('sqs_inbox_duplicates_total');
            return { status: 'DUPLICATE_MESSAGE', idempotentReplay: true };
          }
        }

        const transacoesAnteriores = await gerenteTransacional.execute<LinhaDoBanco[]>(
          'select id,payload_hash,status,failure_code,result_balance::text,currency from wager_transaction where provider_id=? and idempotency_key=? for update',
          [providerId, idempotencyKey],
        );
        if (transacoesAnteriores[0]) {
          const transacaoAnterior = transacoesAnteriores[0];
          if (transacaoAnterior.payload_hash !== payloadHash) {
            throw new ConflictException({ code: 'IDEMPOTENCY_KEY_REUSED' });
          }
          incrementMetric('idempotent_replays_total');
          if (messageId) {
            await gerenteTransacional.execute(
              'update inbox_message set processed_at=now() where consumer_name=? and message_id=?',
              ['wager-transactions', messageId],
            );
          }
          return this.montarRespostaDeReplay(transacaoAnterior);
        }

        const transactionId = randomUUID();
        const carteiras = await gerenteTransacional.execute<LinhaDoBanco[]>(
          'select id,player_id,currency,balance::text,version from wallet where id=? for update',
          [walletId],
        );
        const carteira = carteiras[0];
        if (!carteira) throw new NotFoundException({ code: 'WALLET_NOT_FOUND' });
        if (carteira.player_id !== playerId) throw new ConflictException({ code: 'PLAYER_WALLET_MISMATCH' });

        const saldoAntes = Money.from({ amount: String(carteira.balance), currency: String(carteira.currency) });
        if (money.currency !== saldoAntes.currency) throw new ConflictException({ code: 'CURRENCY_MISMATCH' });
        let status = 'PROCESSED';
        let failureCode: string | null = null;
        let balanceAfter = saldoAntes;
        let direction: 'DEBIT' | 'CREDIT' | undefined;
        let referenceId: string | null = null;

        let reference: LinhaDoBanco | undefined;
        if (referenceExternalId) {
          const referencias = await gerenteTransacional.execute<LinhaDoBanco[]>(`select id,kind,status,amount::text,currency,player_id,wallet_id,round_id from wager_transaction where provider_id=? and external_transaction_id=? for update`, [providerId, referenceExternalId]);
          reference = referencias[0];
          if (!reference) status = 'PENDING_REFERENCE';
          else if (reference.status === 'PENDING' || reference.status === 'PENDING_REFERENCE') status = 'PENDING_REFERENCE';
          else {
            const isReversal = kind === 'REFUND' || kind === 'ROLLBACK';
            const sameContext = reference.player_id === playerId && reference.wallet_id === walletId && reference.round_id === roundId && reference.currency === money.currency;
            if (!sameContext || isReversal && String(reference.amount) !== money.toJSON().amount) { status = 'REJECTED'; failureCode = 'INVALID_REFERENCE'; }
            else {
              referenceId = String(reference.id);
              if (reference.status !== 'PROCESSED') { status = 'REJECTED'; failureCode = 'INVALID_REFERENCE'; }
              else if (kind === 'REFUND' && reference.kind !== 'BET' || kind === 'ROLLBACK' && !['BET', 'WIN', 'REFUND'].includes(String(reference.kind)) || kind === 'WIN' && reference.kind !== 'BET') { status = 'REJECTED'; failureCode = 'INVALID_REFERENCE_KIND'; }
            }
            if (status === 'PROCESSED') {
              if (isReversal) {
                const reversoesExistentes = await gerenteTransacional.execute<LinhaDoBanco[]>(
                  'select id from wager_transaction where reference_external_transaction_id=? and provider_id=? and kind=? and status in (\'PROCESSED\',\'PENDING_REFERENCE\') limit 1',
                  [referenceExternalId, providerId, kind],
                );
                if (reversoesExistentes.length) { status = 'REJECTED'; failureCode = 'REFERENCE_ALREADY_REVERSED'; }
              }
              if (status === 'PROCESSED') direction = kind === 'WIN' || kind === 'REFUND' || reference.kind === 'BET' ? 'CREDIT' : 'DEBIT';
            }
          }
        } else if (kind === 'BET') direction = 'DEBIT';
        else if (kind === 'WIN') direction = 'CREDIT';

        if (status === 'PROCESSED' && direction === 'DEBIT') {
          try { balanceAfter = saldoAntes.subtract(money); }
          catch { status = 'REJECTED'; failureCode = referenceId ? 'REVERSAL_WOULD_OVERDRAW' : 'INSUFFICIENT_FUNDS'; }
        } else if (status === 'PROCESSED' && direction === 'CREDIT') balanceAfter = saldoAntes.add(money);

        await gerenteTransacional.execute(`insert into wager_transaction(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,reference_external_transaction_id,reference_transaction_id,status,failure_code,result_balance,processed_at,reference_next_attempt_at,reference_expires_at)
          values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,case when ? in ('PROCESSED','REJECTED') then now() else null end,
            case when ?='PENDING_REFERENCE' then now()+interval '2 seconds' else null end,
            case when ?='PENDING_REFERENCE' then now()+interval '24 hours' else null end)`, [transactionId, providerId, externalId, idempotencyKey, payloadHash, walletId, playerId, roundId, gameId, kind, money.toJSON().amount, money.currency, referenceExternalId ?? null, referenceId, status, failureCode, balanceAfter.toJSON().amount, status, status, status]);

        if (status === 'PROCESSED' && direction) {
          await gerenteTransacional.execute('update wallet set balance=?,version=version+1,updated_at=now() where id=?', [balanceAfter.toJSON().amount, walletId]);
          await gerenteTransacional.execute('insert into wallet_ledger_entry(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after) values(?,?,?,?,?,?,?,?)', [randomUUID(), walletId, transactionId, direction, money.toJSON().amount, money.currency, saldoAntes.toJSON().amount, balanceAfter.toJSON().amount]);
          await gerenteTransacional.execute('insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)', [randomUUID(), walletId, 'WalletBalanceChanged', JSON.stringify({ walletId, transactionId, direction, money: money.toJSON(), balanceBefore: saldoAntes.toJSON(), balanceAfter: balanceAfter.toJSON(), walletVersion: Number(carteira.version) + 1 }), requestCorrelationId ?? transactionId, messageId ?? transactionId]);
        }
        if (status === 'PROCESSED' || status === 'REJECTED' || status === 'PENDING_REFERENCE') {
          const eventType = status === 'PROCESSED' ? 'WagerTransactionProcessed' : status === 'REJECTED' ? 'WagerTransactionRejected' : 'WagerTransactionPendingReference';
          await gerenteTransacional.execute('insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)', [randomUUID(), walletId, eventType, JSON.stringify({ transactionId, providerId, externalTransactionId: externalId, walletId, kind, status, failureCode, referenceExternalTransactionId: referenceExternalId ?? null }), requestCorrelationId ?? transactionId, messageId ?? transactionId]);
        }
        if (messageId) {
          await gerenteTransacional.execute('update inbox_message set processed_at=now() where consumer_name=? and message_id=?', ['wager-transactions', messageId]);
        }
        return { transactionId, status, failureCode, balance: balanceAfter.toJSON(), idempotentReplay: false };
      });
    } catch (error) {
      const replayDaCorrida = await this.traduzirFalhaDoBanco(error, dados);
      if (replayDaCorrida === undefined) throw error;
      resultado = replayDaCorrida;
    }

    return resultado;
  }

  /** Converte corrida de unicidade em replay ou conflito sem iniciar novo processamento. */
  private async traduzirFalhaDoBanco(
    erroOriginal: unknown,
    dados: Pick<DadosDaTransacaoFinanceira, 'providerId' | 'externalId' | 'idempotencyKey' | 'payloadHash'>,
  ): Promise<unknown | undefined> {
    const erroDoBanco = erroOriginal as { code?: string; cause?: { code?: string } };
    const codigoDoErro = erroDoBanco.code ?? erroDoBanco.cause?.code;
    if (codigoDoErro === '40P01' || codigoDoErro === '55P03') incrementMetric('database_lock_conflicts_total');
    if (['08000', '08003', '08006', '53300', '57P01', '40P01', '55P03'].includes(String(codigoDoErro))) {
      throw new ServiceUnavailableException({ code: 'TRANSIENT_DATABASE_FAILURE' });
    }
    if (['22P02', '22003'].includes(String(codigoDoErro))) throw new BadRequestException({ code: 'INVALID_PAYLOAD' });
    if (['23503', '23514'].includes(String(codigoDoErro))) throw new InternalServerErrorException({ code: 'FINANCIAL_CONSTRAINT_VIOLATION' });
    if (codigoDoErro !== '23505') return undefined;

    const linhasEncontradas = await this.orm.em.getConnection().execute<LinhaDoBanco[]>(
      'select id,idempotency_key,payload_hash,status,failure_code,result_balance::text,currency from wager_transaction where (provider_id=? and idempotency_key=?) or (provider_id=? and external_transaction_id=?)',
      [dados.providerId, dados.idempotencyKey, dados.providerId, dados.externalId],
    );
    const anterior = linhasEncontradas[0];
    if (!anterior) return undefined;
    if (anterior.idempotency_key !== dados.idempotencyKey) {
      throw new ConflictException({ code: 'EXTERNAL_TRANSACTION_ID_REUSED' });
    }
    if (anterior.payload_hash !== dados.payloadHash) {
      throw new ConflictException({ code: 'IDEMPOTENCY_KEY_REUSED' });
    }
    return this.montarRespostaDeReplay(anterior);
  }

  private montarRespostaDeReplay(transacaoAnterior: LinhaDoBanco): unknown {
    return {
      transactionId: transacaoAnterior.id,
      status: transacaoAnterior.status,
      failureCode: transacaoAnterior.failure_code,
      balance: { amount: transacaoAnterior.result_balance ?? '0.00', currency: transacaoAnterior.currency },
      idempotentReplay: true,
    };
  }
}
