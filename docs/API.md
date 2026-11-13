# API HTTP

The challenge README remains the original specification. This file describes the implementation currently present in this repository.

## Wallets

`POST /wallets` creates one wallet per `playerId` and currency. `initialBalance` is `{ "amount": "100.00", "currency": "BRL" }`. A positive opening balance creates an internal `OPENING` transaction, ledger credit, and outbox events in the same database transaction. Returns `201`; duplicate player/currency returns `409 WALLET_ALREADY_EXISTS`.

`GET /wallets/:walletId` returns the stored balance and version.

`GET /wallets/:walletId/ledger?limit=50&cursor=...` returns newest entries first. The opaque cursor is tied to the `(created_at, id)` order; limit is capped at 100.

`POST /wallets/:walletId/reconciliation` compares stored balance with the exact signed sum of ledger entries. It reports a difference and never repairs the wallet.

## Wager transactions

`POST /wagering/transactions` requires the `Idempotency-Key` header and these fields: `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, and `money`. `REFUND` and `ROLLBACK` also require `referenceExternalTransactionId`. Money amounts are decimal strings with exactly two fractional digits. `LOSS` accepts only `0.00`.

Responses:

- `200`: `PROCESSED`; identical retries return the persisted transaction and balance snapshot with `idempotentReplay: true`.
- `202`: `PENDING_REFERENCE`; the referenced transaction has not arrived yet.
- `422`: `REJECTED` by a financial/domain rule, with `failureCode`.
- `400`: malformed payload, amount, kind, or missing idempotency key.
- `404`: wallet not found.
- `409`: idempotency key reused with a different payload, player/wallet mismatch, currency mismatch, duplicate external identity, or wallet conflict.

Queries: `GET /wagering/transactions/:transactionId` and `GET /providers/:providerId/wagering/transactions/:externalTransactionId`.

## Health

- `GET /health/live` reports that the process is running.
- `GET /health/ready` checks PostgreSQL, the SQS events queue and the transactions queue.
- `GET /health/metrics` returns Prometheus text for transaction statuses, idempotent replays, inbox duplicates, retries, DLQ depth, lock conflicts, outbox lag and processing latency.

## SQS transaction input

Send `WagerTransactionRequested` messages to `wager-transactions.fifo` with a stable `messageId` and `idempotencyKey`:

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

Set the FIFO `MessageGroupId` to the wallet ID (to preserve wallet order) and `MessageDeduplicationId` to the stable message ID. PostgreSQL locks and constraints remain the correctness boundary; broker ordering is only an optimization.

The consumer routes the payload through the wager handler. It inserts the inbox record in the same PostgreSQL transaction as the financial operation and deletes the SQS message only after the handler commits. Redelivery after commit is deduplicated by `(consumer_name, message_id)`. Malformed messages are copied to `wager-transactions-dlq.fifo`; transient failures use exponential visibility backoff and the configured SQS redrive policy.

`PENDING_REFERENCE` records are retried by the scheduler with exponential backoff, a maximum of 10 attempts, and a 24-hour TTL. The first limit reached rejects the transaction with `REFERENCE_NOT_FOUND` and writes a rejection event.

## Validation status and remaining work

The current Docker build and liveness/readiness endpoints have been checked. The automated integration suite still needs scenarios for SQS inbox/redelivery, retries/DLQ, concurrent publishers, scheduler resolution, and crash recovery. OIDC is intentionally not implemented because authentication is optional in the challenge; use an external IdP before production.
