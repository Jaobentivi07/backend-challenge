# Decisões de arquitetura

## Escopo implementado

- NestJS com TypeScript estrito, Bun como runtime/package manager, MikroORM 6 e PostgreSQL.
- `Money` encapsula `decimal.js`; contratos monetários usam strings decimais e a aplicação valida o formato lexical antes de construir o Decimal.
- Cada escrita financeira executa numa transação PostgreSQL. A linha da carteira é lida com `FOR UPDATE`; o bloqueio é por carteira, permitindo carteiras distintas em paralelo.
- O banco impõe saldo não negativo, uma carteira por jogador/moeda, idempotência única por provedor, um lançamento por transação/carteira e imutabilidade do ledger. Triggers deferred verificam no commit a correspondência transaction↔ledger e saldo↔ledger.
- A resposta da operação idempotente mantém o saldo observado no veredito em `result_balance`, sem recalculá-lo em replays posteriores.
- A API inclui criação/consulta de carteira, submissão de wager, consulta de transação, ledger paginado e reconciliação.
- `ControladorFinanceiro` valida e traduz o contrato HTTP; `ProcessadorDeTransacoesFinanceiras` coordena a transação, idempotência, saldo, ledger, inbox e outbox. Os campos definidos pelo desafio, pelo PostgreSQL e pelo SQS mantêm seus nomes de integração.
- O consumer FIFO roteia mensagens para o mesmo handler HTTP interno. `messageId` e hash entram no header interno; a inbox, o wager e os efeitos financeiros são commitados juntos. O ack ocorre após a resposta de commit.
- Mensagens malformadas são enviadas à DLQ; falhas transitórias têm visibility timeout exponencial e a fila aplica redrive após cinco recebimentos. Shutdown gracioso espera o processamento ativo ou devolve mensagens à fila.
- Um scheduler com `FOR UPDATE SKIP LOCKED` reavalia referências pendentes. Usa backoff exponencial, até 10 tentativas, limitado a 300 segundos por intervalo, e TTL de 24 horas; o que vencer primeiro leva a rejeição auditável `REFERENCE_NOT_FOUND`.
- O publisher drena até 40 eventos por rodada, limita os lotes a dez mensagens e 1 MiB, e executa no máximo quatro chamadas SQS simultâneas por processo. Usa lease de 30 segundos, `SKIP LOCKED`, uma confirmação/retry SQL em lote por rodada e FIFO por carteira; o `eventId` permanece estável em reenvios.
- `/health/metrics` expõe métricas Prometheus. Logs de aplicação são JSON e não carregam valores financeiros.

## Limitações restantes

- Os testes de integração cobrem falha com `SIGKILL` após o commit, recuperação por um consumidor substituto, DLQ manual, retry de visibilidade e redrive automático no LocalStack. Ainda falta testar uma indisponibilidade real de rede entre o serviço e o SQS.
- Este repositório não contém consumidores downstream da fila de eventos. Cada consumidor precisa de uma inbox própria com unicidade por `eventId`, confirmada atomicamente com os efeitos, pois a deduplicação FIFO do SQS tem duração limitada.
- Métricas de replay, retry SQS e conflitos de lock são contadores em memória por processo; os status, tentativas do scheduler/outbox, latência e lag são lidos do banco. Use Prometheus para coletar cada instância e agregue os contadores localmente.
- A entrada SQS ainda faz loopback HTTP para reutilizar a rota de wager; worker e API são co-localizados no processo/container. O fluxo de referência pendente continua no scheduler próprio e ainda não usa o novo processador compartilhado.
- O consumer processa sequencialmente as mensagens de cada recebimento. A vazão desse caminho ainda não foi medida; não há base para aumentar concorrência sem avaliar a ordem FIFO e o limite por carteira.
- OIDC continua fora do escopo pontuado do desafio. Antes de produção, integrar um IdP e validar a identidade/autorização de cada provedor.

## Regras de referência

- `BET` debita, `WIN` credita e `LOSS` registra sem mexer no saldo.
- `REFUND` credita a aposta referenciada; `ROLLBACK` inverte o efeito financeiro da operação referenciada.
- Referência ausente fica pendente. Referência inválida, repetida ou que cause saldo negativo é rejeitada com código estável.
- A unicidade final é garantida no PostgreSQL; locks de aplicação/broker não são usados como fonte de verdade.

## Situação atual e prioridades

Estado em 13/11/2026: migrations 20261112000000 a 20261115000000 aplicam o schema financeiro, inbox/outbox, agendamento de referência, envelope de correlação e triggers deferred. Build e testes passaram nesta revisão: 14 testes Node, 3 testes Bun de `Money` e 13 testes de integração com PostgreSQL/LocalStack, com 79 verificações. A indisponibilidade real de rede entre o serviço e o SQS continua sem teste. Isso não substitui uma validação em infraestrutura de produção.

Regras já protegidas no schema:

### Garantias no PostgreSQL

- FK composta wallet/player/currency, unicidade de wallet por player/currency, idempotência provider/key e identidade externa provider/transaction.
- Ledger append-only, uma linha por transaction/wallet, aritmética antes/depois, saldo não negativo e reconciliação deferred no commit.
- Referência de mesmo provider/player/wallet/currency/round, tipo de referência permitido, valor integral em refund/rollback e proteção contra self-reference.
- Transição terminal de status bloqueada e consistência de cardinalidade/valores entre transaction e ledger.
- Idempotency payload hash SHA-256 de JSON canônico; `Idempotency-Key` fica fora do hash e precisa coincidir com a chave no envelope SQS.

## Comparação: fork tcalgarotto/backend-challenge

Referências consultadas: [repositório e README](https://github.com/tcalgarotto/backend-challenge) e [ARCHITECTURE.md no branch `delivery`](https://github.com/tcalgarotto/backend-challenge/blob/delivery/ARCHITECTURE.md). O projeto declara Bun, NestJS, MikroORM, PostgreSQL, LocalStack/SQS FIFO, três réplicas, workers, migrations, testes e evidências de carga. O README mapeia explicitamente requisitos para componentes e testes; a arquitetura descreve transações, locks, inbox/outbox, scheduler, leases e constraints.

### O que vale adotar como referência

- Um único caso de uso de wager compartilhado por HTTP, SQS e scheduler, com adapters fora do domínio.
- Locks por carteira, além de constraints no PostgreSQL; o broker não é a garantia financeira.
- Inbox e outbox na transação financeira, ack apenas depois do commit e publisher recuperável após crash.
- Snapshots idempotentes, referências com prazo/backoff e evidência de testes contra PostgreSQL/SQS reais.
- ADRs e matriz requisito → implementação → teste. Isso facilita revisão e torna trade-offs verificáveis.

### Risco de domínio observado, não defeito confirmado

O fork declara `per_type` como política padrão: uma referência pode receber um `REFUND` e um `ROLLBACK`, embora não possa receber dois do mesmo tipo. O próprio README mostra o caso em que ambos revertem a mesma BET e o saldo recebe o crédito duas vezes; também oferece `per_reference` para impedir isso. A regra literal do desafio proíbe repetição do mesmo tipo, então `per_type` é uma interpretação possível, mas pode divergir da expectativa operacional de um provedor. Para este serviço, manteremos inicialmente a regra do enunciado e registraremos a semântica como decisão explícita antes de permitir `REFUND` e `ROLLBACK` combinados.

O fork também fixa `LOSS` em `0.00` e sem referência. Como o enunciado define LOSS como resultado sem movimento monetário, adotamos essa interpretação nesta base. As métricas e resultados de testes de carga citados pelo fork são declarações do próprio repositório; não foram reproduzidos independentemente aqui.

### Limite da comparação

Foi possível analisar README, arquitetura e estrutura declarada do fork; não fiz auditoria linha a linha nem executei o fork. Portanto não atribuo bugs de implementação aos autores. A principal oportunidade para nosso projeto é reduzir o atual hiato de escopo e provar suas garantias com a mesma disciplina documental e testes reais.

## Execução local

1. Copie `.env.example` para `.env`.
2. Inicie PostgreSQL: `docker compose up -d postgres`.
3. Instale dependências com Bun: `bun install`.
4. Aplique migrations: `bun run migration:up`.
5. Inicie a API: `bun run start:dev`.

Health: `GET /health/live` e `GET /health/ready`.

Contrato HTTP atual: [docs/API.md](docs/API.md). O README original continua preservando o enunciado do desafio.

Testes de integração: inicie um PostgreSQL descartável em `localhost:5432`, defina `TEST_DATABASE_URL` para esse banco e rode `bun run test:integration`. O script compila, aplica migrations e sobe três processos NestJS em portas separadas; não aponta a suíte para dados de desenvolvimento/produção.
