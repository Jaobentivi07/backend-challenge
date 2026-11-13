import { Body, ConflictException, Controller, Get, Headers, NotFoundException, Param, Post, BadRequestException, Query, Res, Inject, Logger } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { createHash, randomUUID } from 'node:crypto';
import { Money, MoneyProps } from './domain/money';
import { incrementMetric } from './observability/metrics';
import { ProcessadorDeTransacoesFinanceiras } from './application/processador-de-transacoes-financeiras';

type LinhaDoBanco = Record<string, unknown>;
interface HttpResponse { status(code: number): HttpResponse }
type TipoDeTransacao = 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
const serializarJsonCanonico = (valor: unknown): string => {
  if (Array.isArray(valor)) return `[${valor.map(serializarJsonCanonico).join(',')}]`;
  if (valor && typeof valor === 'object') return `{${Object.entries(valor as Record<string, unknown>).sort(([chaveA], [chaveB]) => chaveA.localeCompare(chaveB)).map(([chave, conteudo]) => `${JSON.stringify(chave)}:${serializarJsonCanonico(conteudo)}`).join(',')}}`;
  return JSON.stringify(valor);
};
const gerarHashDoPayload = (dadosDoPayload: unknown): string => createHash('sha256').update(serializarJsonCanonico(dadosDoPayload)).digest('hex');
const exigirTextoNaoVazio = (valor: unknown, nomeDoCampo: string, tamanhoMaximo = 191): string => {
  if (typeof valor !== 'string' || !valor.trim() || valor.length > tamanhoMaximo) throw new BadRequestException({ code: 'INVALID_PAYLOAD', field: nomeDoCampo });
  return valor;
};
const validarIdentificadorUuid = (valor: string, nomeDoCampo: string): string => {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(valor)) throw new BadRequestException({ code: 'INVALID_UUID', field: nomeDoCampo });
  return valor;
};
const permitirSomenteCampos = (valor: Record<string, unknown>, camposPermitidos: string[], nomeDoObjeto: string): void => {
  if (!valor || typeof valor !== 'object' || Array.isArray(valor)) throw new BadRequestException({ code: 'INVALID_PAYLOAD', field: nomeDoObjeto });
  const campoDesconhecido = Object.keys(valor).find((chave) => !camposPermitidos.includes(chave));
  if (campoDesconhecido) throw new BadRequestException({ code: 'UNKNOWN_FIELD', field: `${nomeDoObjeto}.${campoDesconhecido}` });
};
const converterParaValorMonetario = (propriedadesMonetarias: MoneyProps): Money => {
  try { return Money.from(propriedadesMonetarias); }
  catch { throw new BadRequestException({ code: 'INVALID_MONEY' }); }
};

@Controller()
export class ControladorFinanceiro {
  private readonly logger = new Logger(ControladorFinanceiro.name);
  constructor(
    @Inject(MikroORM) private readonly orm: MikroORM,
    private readonly processadorDeTransacoes: ProcessadorDeTransacoesFinanceiras,
  ) {}

  @Post('wallets')
  /** Cria a carteira e grava saldo inicial, ledger e eventos na mesma transação. */
  async criarCarteira(@Body() body: Record<string, unknown>): Promise<unknown> {
    permitirSomenteCampos(body, ['playerId', 'initialBalance'], 'body');
    const playerId = exigirTextoNaoVazio(body.playerId, 'playerId');
    validarIdentificadorUuid(playerId, 'playerId');
    const initial = body.initialBalance as MoneyProps;
    if (!initial || typeof initial !== 'object') throw new BadRequestException({ code: 'INVALID_PAYLOAD', field: 'initialBalance' });
    permitirSomenteCampos(initial as unknown as Record<string, unknown>, ['amount', 'currency'], 'initialBalance');
    const opening = converterParaValorMonetario(initial);
    if (opening.isNegative()) throw new BadRequestException({ code: 'INVALID_MONEY' });
    const id = randomUUID();
    try {
      await this.orm.em.transactional(async (gerenteTransacional) => {
        await gerenteTransacional.execute('insert into wallet(id,player_id,currency,balance,version) values(?,?,?,?,1)', [id, playerId, opening.currency, opening.toJSON().amount]);
        if (opening.isPositive()) {
          const idDaTransacao = randomUUID();
          await gerenteTransacional.execute(`insert into wager_transaction(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,status,result_balance,processed_at)
            values(?, 'internal', ?, ?, ?, ?, ?, 'opening', 'system', 'OPENING', ?, ?, 'PROCESSED', ?, now())`, [idDaTransacao, idDaTransacao, `opening:${id}`, gerarHashDoPayload({ id, opening: opening.toJSON() }), id, playerId, opening.toJSON().amount, opening.currency, opening.toJSON().amount]);
          await gerenteTransacional.execute(`insert into wallet_ledger_entry(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after) values(?,?,?,'CREDIT',?,?,0,?)`, [randomUUID(), id, idDaTransacao, opening.toJSON().amount, opening.currency, opening.toJSON().amount]);
          await gerenteTransacional.execute(`insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)`, [randomUUID(), id, 'WalletBalanceChanged', JSON.stringify({ walletId: id, transactionId: idDaTransacao, direction: 'CREDIT', money: opening.toJSON(), balanceBefore: { amount: '0.00', currency: opening.currency }, balanceAfter: opening.toJSON(), walletVersion: 1 }), idDaTransacao, idDaTransacao]);
          await gerenteTransacional.execute(`insert into outbox_message(id,aggregate_id,event_type,payload,occurred_at,correlation_id,causation_id) values(?,?,?,?::jsonb,now(),?,?)`, [randomUUID(), id, 'WagerTransactionProcessed', JSON.stringify({ transactionId: idDaTransacao, kind: 'OPENING', status: 'PROCESSED' }), idDaTransacao, idDaTransacao]);
        }
      });
    } catch (error) {
      if (String(error).includes('wallet_player_id_currency_key')) throw new ConflictException({ code: 'WALLET_ALREADY_EXISTS' });
      throw error;
    }
    return { id, playerId, balance: opening.toJSON(), version: 1 };
  }

  @Get('wallets/:walletId')
  /** Consulta o saldo persistido e a versão da carteira. */
  async consultarCarteira(@Param('walletId') walletId: string): Promise<unknown> {
    validarIdentificadorUuid(walletId, 'walletId');
    const rows = await this.orm.em.getConnection().execute<LinhaDoBanco[]>('select id,player_id,currency,balance::text,version from wallet where id=?', [walletId]);
    const row = rows[0];
    if (!row) throw new NotFoundException({ code: 'WALLET_NOT_FOUND' });
    return { id: row.id, playerId: row.player_id, balance: { amount: row.balance, currency: row.currency }, version: row.version };
  }

  @Get('wallets/:walletId/ledger')
  /** Lista lançamentos em ordem estável, mantendo a precisão temporal no cursor. */
  async consultarLancamentos(@Param('walletId') walletId: string, @Query('cursor') cursor?: string, @Query('limit') limitRaw?: string): Promise<unknown> {
    validarIdentificadorUuid(walletId, 'walletId');
    const limit = Math.min(100, Math.max(1, Number.parseInt(limitRaw ?? '50', 10) || 50));
    let rows: LinhaDoBanco[];
    if (cursor) {
      let decoded: { at: string; id: string };
      try { decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as { at: string; id: string }; }
      catch { throw new BadRequestException({ code: 'INVALID_CURSOR' }); }
      if (!decoded || typeof decoded !== 'object' || typeof decoded.at !== 'string' || Number.isNaN(Date.parse(decoded.at)) || typeof decoded.id !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(decoded.id)) throw new BadRequestException({ code: 'INVALID_CURSOR' });
      rows = await this.orm.em.getConnection().execute<LinhaDoBanco[]>(`select id,transaction_id,direction,amount::text,currency,balance_before::text,balance_after::text,created_at::text as created_at from wallet_ledger_entry where wallet_id=? and (created_at,id)<(?::timestamptz,?::uuid) order by created_at desc,id desc limit ?`, [walletId,decoded.at,decoded.id,limit + 1]);
    } else {
      rows = await this.orm.em.getConnection().execute<LinhaDoBanco[]>(`select id,transaction_id,direction,amount::text,currency,balance_before::text,balance_after::text,created_at::text as created_at from wallet_ledger_entry where wallet_id=? order by created_at desc,id desc limit ?`, [walletId,limit + 1]);
    }
    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const last = page[page.length - 1];
    return { items: page.map((row) => ({ id: row.id, transactionId: row.transaction_id, direction: row.direction, money: { amount: row.amount, currency: row.currency }, balanceBefore: { amount: row.balance_before, currency: row.currency }, balanceAfter: { amount: row.balance_after, currency: row.currency }, createdAt: row.created_at })), nextCursor: hasMore && last ? Buffer.from(JSON.stringify({ at: String(last.created_at), id: last.id })).toString('base64url') : null };
  }

  @Post('wallets/:walletId/reconciliation')
  /** Compara saldo da carteira com a soma assinada dos lançamentos. */
  async reconciliarCarteira(@Param('walletId') walletId: string): Promise<unknown> {
    validarIdentificadorUuid(walletId, 'walletId');
    const rows = await this.orm.em.getConnection().execute<LinhaDoBanco[]>(`select w.currency,w.balance::text as stored, coalesce(sum(case when l.direction='CREDIT' then l.amount else -l.amount end),0)::numeric(19,2)::text as calculated,count(l.id)::int as entries from wallet w left join wallet_ledger_entry l on l.wallet_id=w.id where w.id=? group by w.id`, [walletId]);
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
  /** Consulta uma transação pelo identificador interno. */
  async consultarTransacao(@Param('transactionId') transactionId: string): Promise<unknown> {
    validarIdentificadorUuid(transactionId, 'transactionId');
    const rows = await this.orm.em.getConnection().execute<LinhaDoBanco[]>('select id,provider_id,external_transaction_id,kind,status,failure_code,wallet_id,amount::text,currency,result_balance::text,created_at,processed_at from wager_transaction where id=?', [transactionId]);
    if (!rows[0]) throw new NotFoundException({ code: 'TRANSACTION_NOT_FOUND' });
    return rows[0];
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  /** Consulta uma transação pela identidade externa do provedor. */
  async consultarTransacaoDoProvedor(@Param('providerId') providerId: string, @Param('externalTransactionId') externalId: string): Promise<unknown> {
    exigirTextoNaoVazio(providerId, 'providerId', 64);
    exigirTextoNaoVazio(externalId, 'externalTransactionId', 128);
    const rows = await this.orm.em.getConnection().execute<LinhaDoBanco[]>('select id,provider_id,external_transaction_id,kind,status,failure_code,wallet_id,amount::text,currency,result_balance::text,created_at,processed_at from wager_transaction where provider_id=? and external_transaction_id=?', [providerId,externalId]);
    if (!rows[0]) throw new NotFoundException({ code: 'TRANSACTION_NOT_FOUND' });
    return rows[0];
  }

  @Post('wagering/transactions')
  /** Valida e processa uma transação financeira dentro de uma transação do banco. */
  async receberTransacaoFinanceira(
    @Headers('idempotency-key') keyHeader: string | undefined,
    @Body() body: Record<string, unknown>,
    @Res({ passthrough: true }) response: HttpResponse,
    @Headers('x-sqs-message-id') messageId?: string,
    @Headers('x-sqs-payload-hash') messageHash?: string,
    @Headers('x-correlation-id') requestCorrelationId?: string,
  ): Promise<unknown> {
    const inicioDaRequisicao = Date.now();
    if (!keyHeader) throw new BadRequestException({ code: 'IDEMPOTENCY_KEY_REQUIRED' });
    permitirSomenteCampos(body, ['providerId', 'externalTransactionId', 'playerId', 'walletId', 'roundId', 'gameId', 'kind', 'money', 'referenceExternalTransactionId', 'idempotencyKey'], 'body');
    const providerId = exigirTextoNaoVazio(body.providerId, 'providerId', 64);
    const externalId = exigirTextoNaoVazio(body.externalTransactionId, 'externalTransactionId', 128);
    const playerId = exigirTextoNaoVazio(body.playerId, 'playerId');
    validarIdentificadorUuid(playerId, 'playerId');
    const walletId = validarIdentificadorUuid(exigirTextoNaoVazio(body.walletId, 'walletId'), 'walletId');
    const roundId = exigirTextoNaoVazio(body.roundId, 'roundId', 128);
    const gameId = exigirTextoNaoVazio(body.gameId, 'gameId', 128);
    const kind = exigirTextoNaoVazio(body.kind, 'kind') as TipoDeTransacao;
    if (!['BET','WIN','LOSS','REFUND','ROLLBACK'].includes(kind)) throw new BadRequestException({ code: 'INVALID_KIND' });
    const moneyProps = body.money as MoneyProps;
    if (!moneyProps || typeof moneyProps !== 'object') throw new BadRequestException({ code: 'INVALID_MONEY' });
    permitirSomenteCampos(moneyProps as unknown as Record<string, unknown>, ['amount', 'currency'], 'money');
    const money = converterParaValorMonetario(moneyProps);
    if (kind === 'LOSS' ? !money.isZero() : !money.isPositive()) {
      throw new BadRequestException({ code: kind === 'LOSS' ? 'LOSS_AMOUNT_MUST_BE_ZERO' : 'AMOUNT_MUST_BE_POSITIVE' });
    }
    const referenceExternalId = body.referenceExternalTransactionId === undefined ? undefined : exigirTextoNaoVazio(body.referenceExternalTransactionId, 'referenceExternalTransactionId', 128);
    const referenceIsRequired = kind === 'REFUND' || kind === 'ROLLBACK';
    const referenceIsForbidden = kind === 'BET' || kind === 'LOSS';
    if (referenceIsRequired && !referenceExternalId || referenceIsForbidden && referenceExternalId) {
      throw new BadRequestException({ code: referenceIsRequired ? 'REFERENCE_REQUIRED' : 'REFERENCE_NOT_ALLOWED' });
    }
    const idempotencyKey = exigirTextoNaoVazio(keyHeader, 'Idempotency-Key', 191);
    if (body.idempotencyKey !== undefined && body.idempotencyKey !== idempotencyKey) throw new BadRequestException({ code: 'IDEMPOTENCY_KEY_MISMATCH' });
    const payloadHash = gerarHashDoPayload({ providerId, externalId, playerId, walletId, roundId, gameId, kind, money: money.toJSON(), referenceExternalId });

    const resultadoDaTransacao = await this.processadorDeTransacoes.processarTransacao({
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
    });
    const resumoDaTransacao = resultadoDaTransacao as { status: string; transactionId?: string };
    incrementMetric('wager_processing_count_total');
    incrementMetric('wager_processing_duration_ms_total', Date.now() - inicioDaRequisicao);
    this.logger.log(JSON.stringify({ event: 'wager_processing_finished', correlationId: requestCorrelationId ?? resumoDaTransacao.transactionId ?? null, messageId: messageId ?? null, transactionId: resumoDaTransacao.transactionId ?? null, walletId, providerId, status: resumoDaTransacao.status }));
    response.status(resumoDaTransacao.status === 'PENDING_REFERENCE' || resumoDaTransacao.status === 'PENDING' ? 202 : resumoDaTransacao.status === 'REJECTED' ? 422 : 200);
    return resultadoDaTransacao;
  }
}
