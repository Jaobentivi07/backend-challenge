import { Module } from '@nestjs/common';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import config from '../mikro-orm.config';
import { HealthController } from './health.controller';
import { FinancialController } from './financial.controller';
import { OutboxPublisher } from './messaging/outbox-publisher';

@Module({
  imports: [MikroOrmModule.forRoot({ ...config, registerRequestContext: false })],
  controllers: [HealthController, FinancialController],
  providers: [OutboxPublisher],
})
export class AppModule {}
