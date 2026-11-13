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
- `GET /health/ready` checks PostgreSQL and the configured SQS events queue.

## Known implementation gaps

The SQS events publisher is implemented, but the SQS command consumer and inbox transaction are not. Pending references are persisted and surfaced in the outbox, but there is no scheduler to resolve them. The PostgreSQL multi-process integration suite is present under `tests/integration`, but has not yet been run in this environment. Authentication is not implemented; the challenge allows documenting an external OIDC extension instead of building local credentials.
