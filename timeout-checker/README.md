# timeout-checker

Verification timeout and safety net for stuck payments.

## Flow

Own consumer group on `withdraw-requests` (key `payment_id`), [worker pool](../libs/worker-pool/README.md) in `DELAYED` mode. A record is due at `requested_at` + `verification-timeout`. Until then its partition is paused (`NOT_DUE`), not slept on, so the consumer stays in the group and nothing is read too early.

At the due time, consistent read of `PAYMENT#`:

| Payment | Action |
|---|---|
| `PENDING` | reject `VERIFICATION_TIMEOUT`: refund, payment `REJECTED` |
| `EXECUTING`, `executing_at` + `executing-stuck-after` passed | republish `transfer-requests` (acked), `stuck_executing_redriven_total` + 1 |
| `EXECUTING`, newer | check again at `executing_at` + `executing-stuck-after` |
| final, or not found | nothing |

- A re-check releases its permit and holds only the commit of its partition, not later records. After a crash the record is redelivered and checked again.
- Reject and republish are safe to repeat: the reject is conditional on `PENDING`, the executor skips payments that are not `EXECUTING` and calls TransferService with the same idempotency key.
- One republish per payment. A payment still stuck after that is left to alerts and the later reconciliation job.
- A replayed withdraw keeps its `requested_at`, so a late replay is rejected at once and refunded.

## Failures

Worker pool failure policy (plan 5.6): transient errors (throttling, conflicts, failed publish) are retried without limit; a bad record or a bug past its budget puts the account `ON_HOLD` with `hold_ref = withdraw-requests/<partition>/<offset>` and the payment is left as it is. Recovery: fix, release the account, then replay from `hold_ref` (see the [worker pool](../libs/worker-pool/README.md#recovering-a-hold)).

## Metrics

`stuck_executing_redriven_total` (alert on any increase), plus the worker pool metrics. Consumer lag of about `verification-timeout` is normal: alert on `worker_oldest_uncommitted_offset_age_seconds` above timeout + limit + margin (about 1h20m), not on lag.

## Configuration

`checker.*` in `application.yaml`:

| Key | Default | Notes |
|---|---|---|
| `verification-timeout` | 1h | |
| `executing-stuck-after` | 15m | |
| `workers.max-in-flight` | 256 | records being checked at once |
| `workers.unknown-max-attempts` / `unknown-max-elapsed` | 10 / 5m | budget for unknown errors |

## Run

`./gradlew :timeout-checker:bootRun` (port 8086, management 8087). Needs DynamoDB Local and Kafka (`docker compose up -d`).
