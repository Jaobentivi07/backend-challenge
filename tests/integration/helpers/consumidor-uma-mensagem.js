const {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient,
} = require('@aws-sdk/client-sqs');
const { ConsumidorDeTransacoes } = require('../../../dist/src/messaging/transaction-consumer.js');

async function executarConsumidorUmaMensagem() {
  const clienteSqs = new SQSClient({
    region: process.env.SQS_REGION ?? 'us-east-1',
    endpoint: process.env.SQS_ENDPOINT_URL,
    credentials: {
      accessKeyId: process.env.SQS_ACCESS_KEY ?? 'test',
      secretAccessKey: process.env.SQS_SECRET_KEY ?? 'test',
    },
  });
  const consumidor = new ConsumidorDeTransacoes();
  const resposta = await clienteSqs.send(new ReceiveMessageCommand({
    QueueUrl: process.env.SQS_TRANSACTIONS_QUEUE_URL,
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 1,
    VisibilityTimeout: 2,
    MessageSystemAttributeNames: ['ApproximateReceiveCount'],
  }));
  const mensagem = resposta.Messages?.[0];
  if (!mensagem) process.exit(2);

  if (process.env.SIMULAR_QUEDA_APOS_COMMIT === 'true') {
    const enviarComandoSqs = consumidor.client.send.bind(consumidor.client);
    consumidor.client.send = (comando) => {
      if (comando instanceof DeleteMessageCommand) process.kill(process.pid, 'SIGKILL');
      return enviarComandoSqs(comando);
    };
  }

  await consumidor.processarMensagem(mensagem);
  await consumidor.onModuleDestroy();
  clienteSqs.destroy();
}

void executarConsumidorUmaMensagem().catch((erro) => {
  process.stderr.write(`${erro instanceof Error ? erro.name : 'UnknownError'}\n`);
  process.exitCode = 1;
});
