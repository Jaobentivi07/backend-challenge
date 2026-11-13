import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { GetQueueAttributesCommand, SendMessageBatchCommand, SendMessageBatchRequestEntry, SQSClient } from '@aws-sdk/client-sqs';
import { randomUUID } from 'node:crypto';
import { incrementMetric } from '../observability/metrics';
import { resumirErroParaLog } from '../observability/error-summary';

type EventoDaOutbox = {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: unknown;
  occurred_at: Date | string;
  correlation_id?: string | null;
  causation_id?: string | null;
  attempts: number;
};

type ResultadoDaPublicacao = {
  evento: EventoDaOutbox;
  confirmado: boolean;
  codigoDaFalha?: string;
};

type EventoPreparadoParaSqs = {
  evento: EventoDaOutbox;
  entrada: SendMessageBatchRequestEntry;
  tamanhoEmBytes: number;
};

const TAMANHO_MAXIMO_DO_LOTE = 10;
const TAMANHO_MAXIMO_DO_PAYLOAD_SQS = 1_048_576;
const MAXIMO_DE_EVENTOS_POR_RODADA = 40;
const MAXIMO_DE_LOTES_CONCORRENTES = 4;
const ESPERA_SEM_EVENTOS_MS = 250;

@Injectable()
export class PublicadorDaOutbox implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PublicadorDaOutbox.name);
  private readonly queueUrl = process.env.SQS_EVENTS_QUEUE_URL;
  private readonly client = new SQSClient({
    region: process.env.SQS_REGION ?? 'us-east-1',
    endpoint: process.env.SQS_ENDPOINT_URL,
    credentials: process.env.SQS_ENDPOINT_URL
      ? { accessKeyId: process.env.SQS_ACCESS_KEY ?? 'test', secretAccessKey: process.env.SQS_SECRET_KEY ?? 'test' }
      : undefined,
  });
  private paradaSolicitada = false;
  private tarefaDeDrenagem?: Promise<void>;

  constructor(@Inject(MikroORM) private readonly orm: MikroORM) {}

  async verificarProntidao(): Promise<void> {
    if (!this.queueUrl) throw new Error('SQS_EVENTS_QUEUE_URL is unset');
    await this.client.send(new GetQueueAttributesCommand({ QueueUrl: this.queueUrl, AttributeNames: ['QueueArn'] }));
  }

  onModuleInit(): void {
    if (!this.queueUrl) {
      this.logger.warn('SQS_EVENTS_QUEUE_URL is unset; outbox publisher is disabled');
      return;
    }
    this.tarefaDeDrenagem = this.drenarContinuamente();
  }

  async onModuleDestroy(): Promise<void> {
    this.paradaSolicitada = true;
    await this.tarefaDeDrenagem;
    this.client.destroy();
  }

  /** Busca continuamente trabalho, aguardando pouco quando a outbox está vazia. */
  private async drenarContinuamente(): Promise<void> {
    while (!this.paradaSolicitada) {
      try {
        const tokenDeLocacao = randomUUID();
        const eventosReservados = await this.orm.em.transactional(async (gerenteTransacional) => gerenteTransacional.execute<EventoDaOutbox[]>(`
          with elegiveis as (
            select candidato.id
            from outbox_message candidato
            where candidato.published_at is null
              and (candidato.next_attempt_at is null or candidato.next_attempt_at <= now())
              and (candidato.lease_until is null or candidato.lease_until < now())
              and not exists (
                select 1 from outbox_message anterior
                where anterior.aggregate_id = candidato.aggregate_id
                  and anterior.published_at is null
                  and anterior.seq < candidato.seq
              )
            order by candidato.seq
            limit ?
            for update skip locked
          )
          update outbox_message as evento
          set lease_token = ?, lease_until = now() + interval '30 seconds', attempts = evento.attempts + 1
          from elegiveis where evento.id = elegiveis.id
          returning evento.id, evento.aggregate_id, evento.event_type, evento.payload, evento.occurred_at,
                    evento.correlation_id, evento.causation_id, evento.attempts
        `, [MAXIMO_DE_EVENTOS_POR_RODADA, tokenDeLocacao]));

        if (!eventosReservados.length) {
          await this.aguardar(ESPERA_SEM_EVENTOS_MS);
          continue;
        }

        const resultados = await this.publicarLotes(eventosReservados);
        await this.confirmarResultados(tokenDeLocacao, resultados);
      } catch (error) {
        // Se a confirmação no banco falhar, os leases expiram e os mesmos eventId serão reenviados.
        this.logger.error(JSON.stringify({ event: 'outbox_batch_failed', ...resumirErroParaLog(error) }));
        await this.aguardar(ESPERA_SEM_EVENTOS_MS);
      }
    }
  }

  /** Envia no máximo quatro lotes em paralelo; cada chamada SQS leva até dez mensagens. */
  private async publicarLotes(eventos: EventoDaOutbox[]): Promise<ResultadoDaPublicacao[]> {
    const eventosPreparados = eventos.map((evento) => {
      const corpoDaMensagem = JSON.stringify({
        eventId: evento.id,
        eventType: evento.event_type,
        aggregateId: evento.aggregate_id,
        correlationId: evento.correlation_id ?? evento.id,
        ...(evento.causation_id ? { causationId: evento.causation_id } : {}),
        occurredAt: new Date(evento.occurred_at).toISOString(),
        version: 1,
        data: evento.payload,
      });
      return {
        evento,
        entrada: {
          Id: evento.id,
          MessageBody: corpoDaMensagem,
          MessageGroupId: evento.aggregate_id,
          MessageDeduplicationId: evento.id,
        },
        tamanhoEmBytes: Buffer.byteLength(corpoDaMensagem, 'utf8'),
      } satisfies EventoPreparadoParaSqs;
    });

    const lotes: EventoPreparadoParaSqs[][] = [];
    const resultados: ResultadoDaPublicacao[] = [];
    let loteAtual: EventoPreparadoParaSqs[] = [];
    let bytesDoLoteAtual = 0;
    for (const preparado of eventosPreparados) {
      if (preparado.tamanhoEmBytes > TAMANHO_MAXIMO_DO_PAYLOAD_SQS) {
        resultados.push({ evento: preparado.evento, confirmado: false, codigoDaFalha: 'SQS_MESSAGE_TOO_LARGE' });
        continue;
      }
      if (loteAtual.length === TAMANHO_MAXIMO_DO_LOTE || bytesDoLoteAtual + preparado.tamanhoEmBytes > TAMANHO_MAXIMO_DO_PAYLOAD_SQS) {
        lotes.push(loteAtual);
        loteAtual = [];
        bytesDoLoteAtual = 0;
      }
      loteAtual.push(preparado);
      bytesDoLoteAtual += preparado.tamanhoEmBytes;
    }
    if (loteAtual.length) lotes.push(loteAtual);

    const resultadosDosLotes: ResultadoDaPublicacao[][] = Array.from({ length: lotes.length }, () => []);
    let proximoLote = 0;
    const publicarProximoLote = async (): Promise<void> => {
      while (proximoLote < lotes.length) {
        const indiceDoLote = proximoLote++;
        resultadosDosLotes[indiceDoLote] = await this.publicarUmLote(lotes[indiceDoLote]);
      }
    };
    const quantidadeDeTrabalhadores = Math.min(MAXIMO_DE_LOTES_CONCORRENTES, lotes.length);
    await Promise.all(Array.from({ length: quantidadeDeTrabalhadores }, () => publicarProximoLote()));
    resultados.push(...resultadosDosLotes.flat());
    return resultados;
  }

  private async publicarUmLote(eventosPreparados: EventoPreparadoParaSqs[]): Promise<ResultadoDaPublicacao[]> {
    const entradas = eventosPreparados.map(({ entrada }) => entrada);

    try {
      const resposta = await this.client.send(new SendMessageBatchCommand({ QueueUrl: this.queueUrl!, Entries: entradas }));
      const idsConfirmados = new Set((resposta.Successful ?? []).map((entrada) => entrada.Id));
      const codigosDeFalha = new Map((resposta.Failed ?? []).map((entrada) => [entrada.Id, entrada.Code ?? 'SQS_BATCH_ENTRY_FAILED']));
      return eventosPreparados.map(({ evento }) => ({
        evento,
        confirmado: idsConfirmados.has(evento.id),
        codigoDaFalha: idsConfirmados.has(evento.id) ? undefined : codigosDeFalha.get(evento.id) ?? 'SQS_BATCH_RESULT_MISSING',
      }));
    } catch (error) {
      const codigoDaFalha = error instanceof Error ? error.name : 'SQS_BATCH_REQUEST_FAILED';
      return eventosPreparados.map(({ evento }) => ({ evento, confirmado: false, codigoDaFalha }));
    }
  }

  /** Confirma todos os sucessos e agenda todos os retries em uma única escrita SQL. */
  private async confirmarResultados(tokenDeLocacao: string, resultados: ResultadoDaPublicacao[]): Promise<void> {
    if (!resultados.length) return;
    const parametrosDaConfirmacao = resultados.flatMap(({ evento, confirmado }) => [
      evento.id,
      tokenDeLocacao,
      confirmado,
      Math.min(300, 2 ** Math.min(Number(evento.attempts), 8)),
    ]);
    const linhasDaConfirmacao = resultados.map(() => '(?::uuid, ?::uuid, ?::boolean, ?::integer)').join(',');

    await this.orm.em.getConnection().execute(`
      update outbox_message as evento
      set published_at = case when resultado.confirmado then now() else evento.published_at end,
          next_attempt_at = case when resultado.confirmado then null
                                else now() + (resultado.atraso_segundos * interval '1 second') end,
          lease_token = null,
          lease_until = null
      from (values ${linhasDaConfirmacao})
        as resultado(id, lease_token, confirmado, atraso_segundos)
      where evento.id = resultado.id
        and evento.lease_token = resultado.lease_token
    `, parametrosDaConfirmacao);

    for (const resultado of resultados) {
      if (resultado.confirmado) continue;
      incrementMetric('outbox_retries_total');
      this.logger.warn(JSON.stringify({
        event: 'outbox_publish_retry',
        eventId: resultado.evento.id,
        walletId: resultado.evento.aggregate_id,
        error: resultado.codigoDaFalha,
      }));
    }
  }

  private async aguardar(milissegundos: number): Promise<void> {
    await new Promise((resolver) => setTimeout(resolver, milissegundos));
  }

}
