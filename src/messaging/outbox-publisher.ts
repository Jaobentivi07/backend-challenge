import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { GetQueueAttributesCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { randomUUID } from 'node:crypto';
import { incrementMetric } from '../observability/metrics';

type OutboxRow = {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: unknown;
  occurred_at: Date | string;
  correlation_id?: string | null;
  causation_id?: string | null;
  attempts: number;
};

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
  private timer?: NodeJS.Timeout;
  private publishing = false;
  private stopping = false;

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
    this.timer = setInterval(() => void this.publicarLoteDeEventos(), 1000);
    void this.publicarLoteDeEventos();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    if (this.timer) clearInterval(this.timer);
    while (this.publishing) await new Promise((resolve) => setTimeout(resolve, 25));
    this.client.destroy();
  }

  private async publicarLoteDeEventos(): Promise<void> {
    if (this.publishing || this.stopping || !this.queueUrl) return;
    this.publishing = true;
    try {
      const tokenDeLocacao = randomUUID();
      const eventosReservados = await this.orm.em.transactional(async (gerenteTransacional) => gerenteTransacional.execute<OutboxRow[]>(`
        with picked as (
          select candidate.id from outbox_message candidate
          where candidate.published_at is null
            and (candidate.next_attempt_at is null or candidate.next_attempt_at <= now())
            and (candidate.lease_until is null or candidate.lease_until < now())
            and not exists (
              select 1 from outbox_message earlier
              where earlier.aggregate_id=candidate.aggregate_id
                and earlier.published_at is null
                and earlier.seq < candidate.seq
            )
          order by candidate.seq
          limit 25
          for update skip locked
        )
        update outbox_message as event
        set lease_token = ?, lease_until = now() + interval '30 seconds', attempts = event.attempts + 1
        from picked where event.id = picked.id
        returning event.id, event.aggregate_id, event.event_type, event.payload, event.occurred_at, event.correlation_id, event.causation_id, event.attempts
      `, [tokenDeLocacao]));

      for (const event of eventosReservados) {
        try {
          const corpoDaMensagem = JSON.stringify({
            eventId: event.id,
            eventType: event.event_type,
            aggregateId: event.aggregate_id,
            correlationId: event.correlation_id ?? event.id,
            ...(event.causation_id ? { causationId: event.causation_id } : {}),
            occurredAt: new Date(event.occurred_at).toISOString(),
            version: 1,
            data: event.payload,
          });
          await this.client.send(new SendMessageCommand({
            QueueUrl: this.queueUrl,
            MessageBody: corpoDaMensagem,
            MessageGroupId: event.aggregate_id,
            MessageDeduplicationId: event.id,
          }));
          await this.orm.em.getConnection().execute(`update outbox_message set published_at=now(),lease_token=null,lease_until=null where id=? and lease_token=?`, [event.id, tokenDeLocacao]);
        } catch (error) {
          incrementMetric('outbox_retries_total');
          const delaySeconds = Math.min(300, 2 ** Math.min(Number(event.attempts), 8));
          await this.orm.em.getConnection().execute(`update outbox_message set next_attempt_at=now()+(? * interval '1 second'),lease_token=null,lease_until=null where id=? and lease_token=?`, [delaySeconds,event.id,tokenDeLocacao]);
          this.logger.warn(JSON.stringify({ event: 'outbox_publish_retry', correlationId: event.id, messageId: null, transactionId: null, walletId: event.aggregate_id, eventId: event.id, error: error instanceof Error ? error.message : 'unknown' }));
        }
      }
    } catch (error) {
      this.logger.error(JSON.stringify({ event: 'outbox_batch_failed', correlationId: null, messageId: null, transactionId: null, error: error instanceof Error ? error.message : 'unknown' }));
    } finally {
      this.publishing = false;
    }
  }
}
