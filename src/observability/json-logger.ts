import { LoggerService } from '@nestjs/common';

export class JsonLogger implements LoggerService {
  log(message: unknown, context?: string): void { this.write('info', message, context); }
  warn(message: unknown, context?: string): void { this.write('warn', message, context); }
  debug(message: unknown, context?: string): void { this.write('debug', message, context); }
  verbose(message: unknown, context?: string): void { this.write('trace', message, context); }
  error(message: unknown, stack?: string, context?: string): void {
    this.write('error', message, context, stack ? { stack } : undefined);
  }

  private write(level: string, message: unknown, context?: string, extra?: Record<string, unknown>): void {
    let payload: Record<string, unknown> = {};
    if (typeof message === 'string' && message.startsWith('{')) {
      try {
        const parsed = JSON.parse(message) as unknown;
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) payload = parsed as Record<string, unknown>;
      } catch { /* retain plain messages */ }
    }
    const structured = {
      timestamp: new Date().toISOString(),
      level,
      message: payload.event ?? (typeof message === 'string' ? message : JSON.stringify(message)),
      context: context ?? null,
      correlationId: null,
      messageId: null,
      transactionId: null,
      walletId: null,
      providerId: null,
      ...payload,
      ...extra,
    };
    const output = JSON.stringify(structured);
    if (level === 'error') console.error(output);
    else console.log(output);
  }
}
