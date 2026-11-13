# Decisões de arquitetura

## Escopo entregue nesta primeira implementação

- NestJS com TypeScript estrito, Bun como runtime/package manager, MikroORM 6 e PostgreSQL.
- `Money` encapsula `decimal.js`; contratos monetários usam strings decimais e a aplicação valida o formato lexical antes de construir o Decimal.
- Cada escrita financeira executa numa transação PostgreSQL. A linha da carteira é lida com `FOR UPDATE`; o bloqueio é por carteira, permitindo carteiras distintas em paralelo.
- O banco impõe saldo não negativo, uma carteira por jogador/moeda, idempotência única por provedor, um lançamento por transação/carteira e imutabilidade do ledger.
- A resposta da operação idempotente mantém o saldo observado no veredito em `result_balance`, sem recalculá-lo em replays posteriores.
- A API inclui criação/consulta de carteira, submissão de wager, consulta de transação, ledger paginado e reconciliação.

## Limitações que permanecem

Este diretório começou apenas com o enunciado. A implementação inicial ainda não fecha o desafio inteiro:

1. SQS de entrada, consumer, inbox no mesmo fluxo financeiro, retries de mensagens e encerramento gracioso ainda não estão conectados. O Compose cria filas FIFO e DLQs com redrive.
2. O publisher SQS da outbox já reivindica linhas com `FOR UPDATE SKIP LOCKED`, lease e backoff, preserva a ordem por carteira e confirma publicação; ainda faltam métricas/alarme de lag e testes de recuperação/concorrência.
3. Transações `PENDING_REFERENCE` são registradas, mas falta o worker agendado para reprocessar, aplicar TTL/backoff e rejeitar referências expiradas.
4. A observabilidade estruturada e as métricas do enunciado ainda precisam ser adicionadas.
5. Cobertura de integração/concorrência com PostgreSQL e LocalStack ainda precisa ser escrita e executada.
6. Autenticação foi deixada fora do escopo inicial, como o enunciado permite; antes de produção deve ser integrado um IdP OIDC e aplicado controle de identidade do provedor.

## Regras de referência

- `BET` debita, `WIN` credita e `LOSS` registra sem mexer no saldo.
- `REFUND` credita a aposta referenciada; `ROLLBACK` inverte o efeito financeiro da operação referenciada.
- Referência ausente fica pendente. Referência inválida, repetida ou que cause saldo negativo é rejeitada com código estável.
- A unicidade final é garantida no PostgreSQL; locks de aplicação/broker não são usados como fonte de verdade.

## Situação atual e prioridades

Estado em 12/11/2026: há um scaffold NestJS/MikroORM e uma primeira API com transações SQL, lock por carteira, `Money`, ledger, migrations e gravação de outbox na mesma transação. `baseUrl` foi removido de `tsconfig.json`: não há aliases configurados, e imports relativos resolvem os módulos. O build NestJS passou com `npm.cmd run build`; quatro testes unitários de `Money` passaram com `npm.cmd run test:node`. A suíte de integração/concorrência foi escrita para três processos reais da API contra PostgreSQL, mas não executada pois Bun, Docker e `psql` não estão instalados. O runner exigido pelo enunciado (`bun test`) está configurado em `test:unit`, `test:integration` e `test:concurrency`. O fork comparado abaixo não foi integrado.

Pendências, em ordem de risco para as invariantes:

### P0 — fechar correção financeira

- Adicionar validação de schema/DTO compartilhada, rejeitar campos desconhecidos e mapear todas as violações de constraint para erros HTTP estáveis.
- Completar barreiras de schema para identidade/contexto de referência, self-reference, transições válidas e consistência transaction↔ledger↔saldo. Já existem FK composta carteira/player/moeda, FK interna de referência, índice de reversão por tipo e bloqueio de UPDATE/DELETE/TRUNCATE do ledger; ainda não há grants separados nem triggers diferidos que comprovem todos os vínculos no commit.
- Conferir as regras de referência pendente e prevenir ciclos/self-reference; validar a migration em PostgreSQL real.
- O snapshot atual salva saldo e estado para replay, mas ainda precisa cobrir todos os campos de resposta e erros de corrida em cenários concorrentes reais.
- `LOSS` aceita somente `0.00`, sem ledger ou alteração de saldo; essa regra já está no controller e na constraint da migration.

### P1 — recuperação distribuída

- Conectar SQS FIFO e consumer ao mesmo caso de uso HTTP; inserir/confirmar inbox na mesma transação e só dar ack após commit.
- Implementar retry transitório, DLQ, limites de tentativa, shutdown gracioso e readiness verificando PostgreSQL e SQS.
- Validar o publisher da outbox contra LocalStack, incluir métricas de lag/tentativa e comprovar crash recovery e concorrência de publishers. O claim ordena a sequência por carteira e não avança enquanto existir evento anterior não publicado.
- Implementar scheduler para `PENDING_REFERENCE`, com backoff, TTL e rejeição auditável após expiração.
- Definir envelope de eventos com eventId, correlationId, version e payload serializado de forma estável.

### P2 — comprovação e operação

- Expandir os quatro testes unitários atuais (somente `Money`). A suíte PostgreSQL/multiprocesso foi escrita e cobre 50 duplicatas paralelas, duas apostas concorrentes, carteiras independentes, replay/conflito e LOSS; falta executá-la. Ainda faltam testes reais de SQS/inbox/outbox, publishers concorrentes, recovery e scheduler.
- Adicionar logs estruturados e métricas obrigatórias; readiness já verifica PostgreSQL e fila de eventos SQS.
- Gerar `bun.lock` e executar migrations, smoke tests e suíte distribuída em ambiente com Bun/Docker. O build foi validado com Node/npm; o runner Bun já está configurado, mas permanece sem execução neste ambiente.
- Avaliar integração OIDC como ponto de extensão; autenticação não é pontuada pelo enunciado. O contrato HTTP atual está em `docs/API.md`.

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
