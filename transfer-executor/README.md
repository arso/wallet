# transfer-executor

Sends the money for approved withdrawals.

## Flow

Consumes `transfer-requests` (key `payment_id`) with the [worker pool](../libs/worker-pool/README.md) in `UNORDERED` mode: each record runs on its own virtual thread, so one slow transfer does not delay other payments.

Per record:

1. Consistent read of `PAYMENT#`. Not `EXECUTING` (already final, duplicate, unknown payment): done. The message is never trusted, only the stored payment.
2. `POST /transfers` to TransferService, `Idempotency-Key: <payment_id>`.
3. Outcome:

| TransferService answer | Result |
|---|---|
| 2xx with `external_tx_id` | complete: ledger `withdraw_completed`, payment `COMPLETED` |
| 422 (clear failure) | reject `EXECUTION_FAILED`: refund, payment `REJECTED` |
| anything else: timeout, connection error, 5xx, other 4xx, bad body | unknown: the money may have been sent. Retry with the same key, exponential backoff + jitter. Still unknown: `NEEDS_REVIEW`, never refunded |

Duplicates are safe: the same idempotency key, the `EXECUTING` check, and the `complete` / `reject` conditions on `EXECUTING`. Two copies of one record end in one ledger entry.

## Circuit breaker

Resilience4j, around each TransferService call. Unknown outcomes count as failures; 2xx and 422 as successes.

- Open: no calls. The pool pauses all partitions (`CIRCUIT_OPEN`). Records in flight fail with `CallNotPermittedException`, a transient error, so they are retried without limit and never end `NEEDS_REVIEW`. After the wait they start with fresh retry attempts.
- Half-open (after `open-wait`): partitions resume; a few calls test the service. Success closes it, failure opens it again.

## Failures

Worker pool failure policy (plan 5.6):

- `complete` failing after TransferService said ok (throttling, conflicts): transient, retried. The retry calls TransferService again with the same key and gets the same answer. Never `NEEDS_REVIEW`.
- Bug (unknown error) past its budget: account `ON_HOLD`, payment stays `EXECUTING`, no refund. The timeout checker re-drives it later.

## Configuration

`executor.*` in `application.yaml`:

| Key | Default | Notes |
|---|---|---|
| `transfer.url` | `http://localhost:8090` | TransferService (the mock locally) |
| `transfer.connect-timeout` / `read-timeout` | 2s / 10s | a timeout is an unknown outcome |
| `transfer.retries` / `initial-backoff` | 5 / 5s | about 2.5 min of retries per payment |
| `workers.max-in-flight` | 256 | a retrying payment holds one permit; size from TransferService capacity and retry time, not CPU |
| `circuit.window-size` / `minimum-calls` / `failure-rate` | 50 / 20 / 50% | count-based window |
| `circuit.open-wait` / `half-open-calls` | 30s / 5 | |

## Run

`./gradlew :transfer-executor:bootRun` (port 8084, management 8085). Needs DynamoDB Local and Kafka (`docker compose up -d`) and the mocks (`./gradlew :mocks:bootRun`).
