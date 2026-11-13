import { Controller, Get, Inject } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { OutboxPublisher } from './messaging/outbox-publisher';

@Controller('health')
export class HealthController {
  constructor(@Inject(MikroORM) private readonly orm: MikroORM, private readonly outbox: OutboxPublisher) {}

  @Get('live')
  live(): { status: string } { return { status: 'ok' }; }

  @Get('ready')
  async ready(): Promise<{ status: string }> {
    await this.orm.em.getConnection().execute('select 1');
    await this.outbox.checkReady();
    return { status: 'ready' };
  }
}
