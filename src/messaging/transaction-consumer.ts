import { DeleteMessageCommand, ReceiveMessageCommand, SQSClient, SendMessageCommand, ChangeMessageVisibilityCommand, GetQueueAttributesCommand } from '@aws-sdk/client-sqs';
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { createHash } from 'node:crypto';
import { incrementMetric } from '../observability/metrics';

type QueueMessage = {
  messageId?: string;
  type?: string;
  correlationId?: string;
  data?: Record<string, unknown>;
};

@Injectable()
export class TransactionConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TransactionConsumer.name);
  private readonly client = new SQSClient({
    region: process.env.SQS_REGION ?? 'us-east-1',
    endpoint: process.env.SQS_ENDPOINT_URL,
    credentials: process.env.SQS_ENDPOINT_URL
      ? { accessKeyId: process.env.SQS_ACCESS_KEY ?? 'test', secretAccessKey: process.env.SQS_SECRET_KEY ?? 'test' }
      : undefined,
  });
  private readonly queueUrl = process.env.SQS_TRANSACTIONS_QUEUE_URL ?? (process.env.SQS_ENDPOINT_URL ? `${process.env.SQS_ENDPOINT_URL}/000000000000/wager-transactions.fifo` : undefined);
  private readonly deadLetterQueueUrl = process.env.SQS_TRANSACTIONS_DLQ_URL ?? (process.env.SQS_ENDPOINT_URL ? `${process.env.SQS_ENDPOINT_URL}/000000000000/wager-transactions-dlq.fifo` : undefined);
  private stopping = false;
  private loop?: Promise<void>;

  onModuleInit(): void {
    if (!this.queueUrl) {
      this.logger.warn('SQS_TRANSACTIONS_QUEUE_URL is unset; transaction consumer is disabled');
      return;
    }
    this.loop = this.consume();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    await this.loop;
    this.client.destroy();
  }

  async checkReady(): Promise<void> {
    if (!this.queueUrl) throw new Error('SQS_TRANSACTIONS_QUEUE_URL is unset');
    await this.client.send(new GetQueueAttributesCommand({ QueueUrl: this.queueUrl, AttributeNames: ['QueueArn'] }));
  }

  async deadLetterDepth(): Promise<number> {
    if (!this.deadLetterQueueUrl) return 0;
    const result = await this.client.send(new GetQueueAttributesCommand({ QueueUrl: this.deadLetterQueueUrl!, AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'] }));
    return Number(result.Attributes?.ApproximateNumberOfMessages ?? 0) + Number(result.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0);
  }

  private async consume(): Promise<void> {
    while (!this.stopping) {
      try {
        const result = await this.client.send(new ReceiveMessageCommand({
          QueueUrl: this.queueUrl!,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 10,
          VisibilityTimeout: 30,
          MessageSystemAttributeNames: ['ApproximateReceiveCount'],
        }));
        for (const message of result.Messages ?? []) {
          if (this.stopping) {
            await this.client.send(new ChangeMessageVisibilityCommand({ QueueUrl: this.queueUrl!, ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: 0 }));
            break;
          }
          await this.processMessage(message);
        }
      } catch (error) {
        this.logger.error(JSON.stringify({ event: 'sqs_receive_failed', correlationId: null, messageId: null, error: error instanceof Error ? error.message : 'unknown' }));
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  private async processMessage(message: { Body?: string; MessageId?: string; ReceiptHandle?: string; Attributes?: Record<string, string> }): Promise<void> {
    const receipt = message.ReceiptHandle;
    const brokerId = message.MessageId ?? 'unknown';
    if (!receipt || !message.Body) return;
    let envelope: QueueMessage | undefined;
    try {
      envelope = JSON.parse(message.Body) as QueueMessage;
      if (envelope.type !== 'WagerTransactionRequested' || !envelope.data || typeof envelope.data !== 'object') throw new Error('INVALID_MESSAGE_ENVELOPE');
      const transaction = envelope.data;
      const idempotencyKey = transaction.idempotencyKey;
      if (typeof idempotencyKey !== 'string' || !idempotencyKey.trim()) throw new Error('IDEMPOTENCY_KEY_REQUIRED');
      const messageId = typeof envelope.messageId === 'string' && envelope.messageId.length ? envelope.messageId : brokerId;
      const payloadHash = createHash('sha256').update(JSON.stringify(envelope)).digest('hex');
      const port = Number(process.env.PORT ?? 3000);
      const response = await fetch(`http://127.0.0.1:${port}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'idempotency-key': idempotencyKey,
          'x-sqs-message-id': messageId,
          'x-sqs-payload-hash': payloadHash,
          'x-correlation-id': envelope.correlationId ?? messageId,
        },
        body: JSON.stringify(transaction),
        signal: AbortSignal.timeout(20_000),
      });
      if (response.status >= 500) {
        incrementMetric('sqs_retries_total');
        await this.retryLater(message, Math.max(1, Number(message.Attributes?.ApproximateReceiveCount ?? 1)));
        return;
      }
      if (response.status === 400) {
        incrementMetric('sqs_dead_lettered_total');
        await this.moveToDeadLetter(message, 'invalid_request');
        return;
      }
      if (response.status < 200 || response.status >= 300) {
        this.logger.warn(JSON.stringify({ event: 'sqs_wager_terminal', correlationId: envelope.correlationId ?? null, messageId, providerId: transaction.providerId ?? null, statusCode: response.status }));
        await this.acknowledge(receipt);
        return;
      }
      const outcome = await response.json() as { transactionId?: string; status?: string };
      this.logger.log(JSON.stringify({ event: 'sqs_wager_committed', correlationId: envelope.correlationId ?? null, messageId, transactionId: outcome.transactionId ?? null, walletId: transaction.walletId ?? null, providerId: transaction.providerId ?? null, status: outcome.status ?? 'duplicate' }));
      await this.acknowledge(receipt);
    } catch (error) {
      if (error instanceof SyntaxError || error instanceof Error && ['INVALID_MESSAGE_ENVELOPE', 'IDEMPOTENCY_KEY_REQUIRED'].includes(error.message)) {
        incrementMetric('sqs_dead_lettered_total');
        await this.moveToDeadLetter(message, 'invalid_envelope');
        return;
      }
      this.logger.warn(JSON.stringify({ event: 'sqs_wager_retry', correlationId: envelope?.correlationId ?? null, messageId: envelope?.messageId ?? brokerId, error: error instanceof Error ? error.message : 'unknown' }));
      incrementMetric('sqs_retries_total');
      await this.retryLater(message, Math.max(1, Number(message.Attributes?.ApproximateReceiveCount ?? 1)));
    }
  }

  private async acknowledge(receipt: string): Promise<void> {
    await this.client.send(new DeleteMessageCommand({ QueueUrl: this.queueUrl!, ReceiptHandle: receipt }));
  }

  private async retryLater(message: { ReceiptHandle?: string; Attributes?: Record<string, string> }, receiveCount: number): Promise<void> {
    if (!message.ReceiptHandle) return;
    const delay = Math.min(60, 2 ** Math.min(receiveCount, 6));
    await this.client.send(new ChangeMessageVisibilityCommand({ QueueUrl: this.queueUrl!, ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: delay }));
  }

  private async moveToDeadLetter(message: { Body?: string; ReceiptHandle?: string; MessageId?: string }, reason: string): Promise<void> {
    if (!message.Body || !message.ReceiptHandle) return;
    await this.client.send(new SendMessageCommand({
      QueueUrl: this.deadLetterQueueUrl!,
      MessageBody: JSON.stringify({ reason, sourceMessageId: message.MessageId, originalBody: message.Body, failedAt: new Date().toISOString() }),
      MessageGroupId: 'invalid-wager-messages',
      MessageDeduplicationId: `${message.MessageId ?? 'unknown'}:${reason}`,
    }));
    await this.acknowledge(message.ReceiptHandle);
    this.logger.error(JSON.stringify({ event: 'sqs_wager_dead_lettered', correlationId: null, messageId: message.MessageId ?? null, reason }));
  }
}
