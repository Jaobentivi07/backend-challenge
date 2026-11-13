import { Module } from '@nestjs/common';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import config from '../mikro-orm.config';
import { HealthController } from './health.controller';
import { ControladorFinanceiro } from './financial.controller';
import { PublicadorDaOutbox } from './messaging/outbox-publisher';
import { ConsumidorDeTransacoes } from './messaging/transaction-consumer';
import { AgendadorDeReferencias } from './messaging/reference-scheduler';
import { ProcessadorDeTransacoesFinanceiras } from './application/processador-de-transacoes-financeiras';

@Module({
  imports: [MikroOrmModule.forRoot({ ...config, registerRequestContext: false })],
  controllers: [HealthController, ControladorFinanceiro],
  providers: [PublicadorDaOutbox, ConsumidorDeTransacoes, AgendadorDeReferencias, ProcessadorDeTransacoesFinanceiras],
})
export class AppModule {}
