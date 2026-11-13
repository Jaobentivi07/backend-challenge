import { Controller, Get, Header, Inject } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { OutboxPublisher } from './messaging/outbox-publisher';
import { TransactionConsumer } from './messaging/transaction-consumer';
import { metricValue } from './observability/metrics';

@Controller('health')
export class HealthController {
  constructor(@Inject(MikroORM) private readonly orm: MikroORM, private readonly outbox: OutboxPublisher, private readonly consumer: TransactionConsumer) {}

  @Get('live')
  live(): { status: string } { return { status: 'ok' }; }

  @Get('ready')
  async ready(): Promise<{ status: string }> {
    await this.orm.em.getConnection().execute('select 1');
    await this.outbox.checkReady();
    await this.consumer.checkReady();
    return { status: 'ready' };
  }

  @Get('metrics')
  @Header('content-type', 'text/plain; version=0.0.4; charset=utf-8')
  async metrics(): Promise<string> {
    const conn = this.orm.em.getConnection();
    const [statuses, retries, lag, latency, deadLetterDepth] = await Promise.all([
      conn.execute<Array<{ status: string; count: string }>>('select status,count(*)::text as count from wager_transaction group by status order by status'),
      conn.execute<Array<{ retries: string }>>(`select (coalesce((select sum(reference_attempts) from wager_transaction),0)
        + coalesce((select sum(greatest(attempts-1,0)) from outbox_message),0))::text as retries`),
      conn.execute<Array<{ seconds: string | null }>>(`select greatest(0,extract(epoch from (now()-min(created_at))))::text as seconds from outbox_message where published_at is null`),
      conn.execute<Array<{ milliseconds: string | null }>>(`select coalesce(avg(extract(epoch from (processed_at-created_at))*1000),0)::text as milliseconds from wager_transaction where processed_at is not null`),
      this.consumer.deadLetterDepth(),
    ]);
    const lines = [
      '# HELP wager_transactions_total Persisted wager transactions by status.',
      '# TYPE wager_transactions_total gauge',
      ...statuses.map((row) => `wager_transactions_total{status="${row.status}"} ${row.count}`),
      '# HELP wager_idempotent_replays_total Idempotent HTTP replays observed by this process.',
      '# TYPE wager_idempotent_replays_total counter',
      `wager_idempotent_replays_total ${metricValue('idempotent_replays_total')}`,
      '# HELP wager_inbox_duplicates_total Redelivered SQS messages deduplicated by the inbox.',
      '# TYPE wager_inbox_duplicates_total counter',
      `wager_inbox_duplicates_total ${metricValue('sqs_inbox_duplicates_total')}`,
      '# HELP wager_sqs_dead_lettered_total Malformed SQS messages explicitly moved to the dead letter queue by this process.',
      '# TYPE wager_sqs_dead_lettered_total counter',
      `wager_sqs_dead_lettered_total ${metricValue('sqs_dead_lettered_total')}`,
      '# HELP wager_retries_total Persisted reference and outbox retry attempts.',
      '# TYPE wager_retries_total counter',
      `wager_retries_total ${Number(retries[0]?.retries ?? 0) + metricValue('sqs_retries_total')}`,
      '# HELP wager_messages_in_dlq Current visible and in-flight messages in the wager DLQ.',
      '# TYPE wager_messages_in_dlq gauge',
      `wager_messages_in_dlq ${deadLetterDepth}`,
      '# HELP wager_database_lock_conflicts_total PostgreSQL lock conflicts observed by this process.',
      '# TYPE wager_database_lock_conflicts_total counter',
      `wager_database_lock_conflicts_total ${metricValue('database_lock_conflicts_total')}`,
      '# HELP wager_reconciliation_mismatches_total Wallet reconciliation mismatches observed by this process.',
      '# TYPE wager_reconciliation_mismatches_total counter',
      `wager_reconciliation_mismatches_total ${metricValue('reconciliation_mismatches_total')}`,
      '# HELP wager_outbox_lag_seconds Age of the oldest unpublished outbox event.',
      '# TYPE wager_outbox_lag_seconds gauge',
      `wager_outbox_lag_seconds ${Number(lag[0]?.seconds ?? 0)}`,
      '# HELP wager_processing_duration_milliseconds_avg Average persisted wager processing latency.',
      '# TYPE wager_processing_duration_milliseconds_avg gauge',
      `wager_processing_duration_milliseconds_avg ${Number(latency[0]?.milliseconds ?? 0)}`,
    ];
    return `${lines.join('\n')}\n`;
  }
}
