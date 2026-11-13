# API HTTP

O README do desafio continua sendo a especificação original. Este documento descreve o comportamento implementado neste repositório.

## Carteiras

`POST /wallets` cria uma carteira por `playerId` e moeda. `initialBalance` segue o formato `{ "amount": "100.00", "currency": "BRL" }`. Um saldo inicial positivo cria a transação `OPENING`, o crédito no ledger e os eventos da outbox na mesma transação do banco. Se qualquer gravação falhar, todas as alterações são revertidas. Jogador/moeda duplicados retornam `409 WALLET_ALREADY_EXISTS`.

`GET /wallets/:walletId` retorna o saldo e a versão da carteira.

`GET /wallets/:walletId/ledger?limit=50&cursor=...` retorna os lançamentos mais recentes primeiro. O cursor opaco preserva a precisão temporal do PostgreSQL e segue a ordenação `(created_at, id)`; o limite máximo é 100.

`POST /wallets/:walletId/reconciliation` compara o saldo persistido com a soma exata e assinada do ledger. Retorna a diferença e não altera a carteira.

## Transações de aposta

`POST /wagering/transactions` exige o cabeçalho `Idempotency-Key` e os campos `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind` e `money`. `REFUND` e `ROLLBACK` exigem `referenceExternalTransactionId`. `WIN` aceita esse campo opcionalmente; quando informado, deve apontar para uma aposta válida no mesmo contexto. `BET` e `LOSS` não aceitam referência. Valores monetários são strings decimais com exatamente duas casas. `LOSS` aceita apenas `0.00`.

Respostas:

- `200`: `PROCESSED`; repetições idênticas retornam a transação e o saldo persistidos, com `idempotentReplay: true`.
- `202`: `PENDING_REFERENCE`; a transação referenciada ainda não chegou.
- `422`: `REJECTED` por uma regra financeira ou de domínio, com `failureCode`.
- `400`: payload, valor ou tipo inválido, ou chave de idempotência ausente.
- `404`: carteira não encontrada.
- `409`: chave de idempotência reutilizada com outro payload, divergência entre jogador e carteira, moeda incompatível, identidade externa duplicada ou conflito de carteira.

Consultas: `GET /wagering/transactions/:transactionId` e `GET /providers/:providerId/wagering/transactions/:externalTransactionId`.

## Saúde e métricas

- `GET /health/live` informa se o processo está em execução.
- `GET /health/ready` verifica PostgreSQL, a fila de eventos SQS e a fila de transações.
- `GET /health/metrics` retorna métricas Prometheus de estados, replays idempotentes, duplicatas da inbox, retries, profundidade da DLQ, conflitos de lock, atraso da outbox e latência.

## Entrada de transações pelo SQS

Envie mensagens `WagerTransactionRequested` para `wager-transactions.fifo` com `messageId` e `idempotencyKey` estáveis:

```json
{
  "messageId": "message-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-11-13T12:00:00.000Z",
  "correlationId": "provider-request-123",
  "data": {
    "idempotencyKey": "provider-a:transaction-123",
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

Defina `MessageGroupId` com o ID da carteira para preservar a ordem por carteira e `MessageDeduplicationId` com o ID estável da mensagem. Locks e restrições do PostgreSQL garantem a consistência; a ordenação do broker é uma otimização.

O consumidor grava a mensagem na inbox dentro da mesma transação PostgreSQL da operação financeira e só remove a mensagem do SQS após o commit. Reentregas após o commit são deduplicadas por `(consumer_name, message_id)`. Mensagens inválidas são copiadas para `wager-transactions-dlq.fifo`; falhas transitórias usam backoff exponencial e a política de redrive configurada no SQS.

O agendador reprocessa registros `PENDING_REFERENCE` com backoff exponencial, até 10 tentativas e TTL de 24 horas. Ao atingir primeiro um desses limites, rejeita a transação com `REFERENCE_NOT_FOUND` e grava um evento de rejeição.

## Validação e pendências

A compilação Docker e os endpoints de liveness/readiness já foram verificados anteriormente. Ainda faltam testes de integração para inbox/reentrega do SQS, retries/DLQ, publishers concorrentes, resolução pelo agendador e recuperação após falhas. OIDC não foi implementado porque autenticação é opcional no desafio; antes de produção, configure um provedor de identidade externo.
