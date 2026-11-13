# Distributed Wagering Processor

Serviço financeiro em NestJS para processar operações de apostas com valores decimais exatos, idempotência persistente e ledger auditável. O saldo é protegido por transações PostgreSQL e locks por carteira.

O enunciado integral do desafio foi preservado em [docs/CHALLENGE.md](docs/CHALLENGE.md). As decisões técnicas e limitações conhecidas estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## O que está implementado

- Criação e consulta de carteiras, com crédito de abertura no ledger.
- Operações `BET`, `WIN`, `LOSS`, `REFUND` e `ROLLBACK` pela API HTTP.
- `Money` imutável com aritmética decimal e contratos em strings, sem `number` para valores monetários.
- Idempotência por provedor e chave, com hash do payload e replay do saldo original.
- Lock PostgreSQL por carteira para serializar alterações concorrentes do mesmo saldo.
- Ledger append-only, reconciliação de saldo e migrations com constraints financeiras.
- Outbox transacional com publisher SQS FIFO, lease recuperável, retry com backoff e ordenação por carteira.
- Consumer SQS FIFO com inbox na transação financeira, deduplicação persistente, retry transitório e DLQ.
- Worker agendado para referências fora de ordem, com backoff exponencial, limite de 10 tentativas e TTL de 24 horas.
- Triggers deferred do PostgreSQL que conferem transaction/ledger e saldo/ledger no commit.
- Logs JSON e métricas operacionais em `/health/metrics`.
- Docker Compose para PostgreSQL, LocalStack/SQS e API.

## Requisitos locais

- Bun 1.x
- Docker Desktop com Docker Compose
- Windows PowerShell, Terminal ou shell compatível

## Subir a aplicação

Clone o repositório, entre na pasta do projeto e execute:

```powershell
Copy-Item .env.example .env
bun install
docker compose up --build
```

O Compose inicia PostgreSQL e LocalStack, cria as filas FIFO e DLQs, aplica as migrations e inicia a API. A aplicação fica disponível em `http://localhost:3000`.

Para parar os serviços:

```powershell
docker compose down
```

Para iniciar apenas o PostgreSQL em segundo plano:

```powershell
docker compose up -d postgres
```

## Testes

### Testes unitários

```powershell
bun run test:unit
```

Os testes de `Money` verificam precisão decimal, entradas inválidas, moedas diferentes e prevenção de saldo negativo. O contrato é exercitado em Bun e Node.js.

### Integração e concorrência

Os testes usam um PostgreSQL descartável e sobem três processos da API. Eles cobrem 50 envios idênticos em paralelo, duas apostas de `80.00` disputando saldo `100.00`, carteiras independentes, replay idempotente, conflito de payload e `LOSS` sem movimento no saldo.

Os testes de integração também verificam paginação do ledger, resolução de referência fora de ordem e publisher contra PostgreSQL e LocalStack. Inicie os dois serviços e configure as URLs descartáveis no PowerShell:

```powershell
docker compose up -d postgres localstack
$env:TEST_DATABASE_URL = "postgresql://wager:wager@localhost:5432/wagering"
$env:SQS_ENDPOINT_URL = "http://localhost:4566"
bun run test:integration
```

Para executar especificamente a suíte de concorrência:

```powershell
bun run test:concurrency
```

Os testes aplicam as migrations no banco informado. Use somente um banco descartável: os testes criam dados e não removem as tabelas ao final.

## Endpoints principais

| Método | Endpoint | Descrição |
| --- | --- | --- |
| `POST` | `/wallets` | Cria uma carteira |
| `GET` | `/wallets/:walletId` | Consulta saldo e versão |
| `GET` | `/wallets/:walletId/ledger` | Consulta lançamentos com cursor |
| `POST` | `/wallets/:walletId/reconciliation` | Compara saldo e ledger |
| `POST` | `/wagering/transactions` | Submete uma operação; exige `Idempotency-Key` |
| `GET` | `/wagering/transactions/:transactionId` | Consulta uma operação pela ID interna |
| `GET` | `/providers/:providerId/wagering/transactions/:externalTransactionId` | Consulta pela identidade do provedor |
| `GET` | `/health/live` | Verifica se o processo está vivo |
| `GET` | `/health/ready` | Verifica PostgreSQL e a fila de eventos SQS |
| `GET` | `/health/metrics` | Métricas Prometheus de transações, retries, DLQ, locks, latência e outbox |

Exemplo de criação de carteira:

```json
{
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "initialBalance": { "amount": "100.00", "currency": "BRL" }
}
```

Exemplo de aposta:

```http
POST /wagering/transactions
Idempotency-Key: provider-a:transaction-123
Content-Type: application/json
```

```json
{
  "providerId": "provider-a",
  "externalTransactionId": "transaction-123",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "roundId": "round-987",
  "gameId": "fortune-chimp",
  "kind": "BET",
  "money": { "amount": "25.00", "currency": "BRL" }
}
```

Mais detalhes dos contratos e códigos de resposta: [docs/API.md](docs/API.md).

## Estrutura do código

```text
src/
  application/            Coordenação transacional de apostas
  domain/                 Money e regras monetárias
  migrations/             Schema PostgreSQL versionado
  messaging/              Publisher, consumidor SQS e worker de referências
  financial.controller.ts API de carteiras, ledger e operações
  health.controller.ts    Health checks
  app.module.ts           Módulo NestJS
  main.ts                 Inicialização da aplicação
tests/
  money.bun.test.mjs      Money sob Bun
  money.test.cjs          Money sob Node.js
  transaction-consumer.test.cjs Testes do consumidor com SQS simulado
  outbox-publisher.test.cjs Testes do publisher com SQS/PostgreSQL simulados
  integration/            PostgreSQL, LocalStack e concorrência multiprocesso
benchmarks/
  outbox-publisher.cjs    Comparação real com PostgreSQL descartável e LocalStack
```

## Testes e benchmark da outbox

Execute `npm.cmd run test:node` para os testes Node.js de Money e do publisher com adaptadores simulados. Eles não medem desempenho real nem substituem integração.

O benchmark compara o publisher sequencial anterior com o publisher em lotes usando um banco PostgreSQL descartável já migrado, sem backlog pendente, e LocalStack. No PowerShell, configure `ALLOW_DISPOSABLE_DB=YES`, `BENCH_DATABASE_URL` e `SQS_ENDPOINT_URL=http://localhost:4566`; então execute `npm.cmd run benchmark:outbox`. `BENCH_EVENTS` controla a quantidade (padrão: 200). A saída é marcada `MEDIDO_REAL_POSTGRESQL_LOCALSTACK`; não rode contra um banco com dados que queira preservar. O benchmark cria filas FIFO temporárias e remove os registros e filas ao terminar. Consulte [benchmarks/RESULTS.md](benchmarks/RESULTS.md) para a última medição real reproduzível.

## Limitações conhecidas

- Os testes cobrem regras financeiras em múltiplos processos, recuperação após `SIGKILL` entre commit e ACK, encaminhamento à DLQ, retry de visibilidade e redrive automático no LocalStack, referências fora de ordem, paginação do ledger e publisher real. Ainda falta testar uma indisponibilidade real de rede entre o serviço e o SQS.
- Esta aplicação publica eventos, mas não inclui consumidores downstream. Cada consumidor precisa deduplicar duravelmente por `eventId` numa inbox própria, gravada na mesma transação que seus efeitos. O FIFO do SQS sozinho não oferece deduplicação durável.
- Os contadores de replay, retry de SQS e conflito de lock são locais a cada processo; os demais gauges são calculados com dados persistidos no PostgreSQL/SQS.
- O consumer SQS processa sequencialmente as mensagens recebidas; a vazão ainda não foi medida.
- Autenticação OIDC não está configurada. O desafio permite deixá-la como ponto de extensão; antes de produção, associe identidade e permissões do provedor a um IdP externo.

Esses limites e as decisões de arquitetura estão detalhados em [ARCHITECTURE.md](ARCHITECTURE.md).
