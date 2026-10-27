# Description

Wallet and payments system. Spec: [docs/payments-system-spec.md](docs/payments-system-spec.md).
Plan: [docs/implementation-plan.md](docs/implementation-plan.md).

## Modules

- `libs/ledger-core` - domain, money transactions, DynamoDB adapter
- `libs/events` - Kafka message records + JSON mapping
- `libs/worker-pool` - Kafka consumer pool: KEYED / UNORDERED / DELAYED dispatch, offset tracking, failure policy, account holds
- `wallet-api` - REST API
- `bridge` - DynamoDB stream to Kafka (`ledger-events`, `withdraw-requests`)
- `completion-service` - saga coordinator: records verification results, rejects or moves to `EXECUTING` and publishes `transfer-requests`
- `transfer-executor` - calls TransferService for `EXECUTING` payments: completes, rejects or marks `NEEDS_REVIEW`; retries and circuit breaker ([details](transfer-executor/README.md))
- `timeout-checker` - rejects payments still `PENDING` at `requested_at` + 1h, re-drives stuck `EXECUTING` payments ([details](timeout-checker/README.md))
- `mocks` - beneficiary, TransferService, verifiers, AML ([rules](mocks/README.md))

## Build and test

`./gradlew clean build` (needs Docker for Testcontainers).

## Run locally

`docker compose up -d` starts DynamoDB Local, Kafka, Jaeger and Prometheus. Then `./gradlew :wallet-api:bootRun`.

Bridge (local runner on the DynamoDB Local stream): `./gradlew :bridge:run`.

Recover a consumer hold (after the fix and the release), from its `hold_ref = <topic>/<partition>/<offset>`:
`./gradlew :libs:worker-pool:run --args="--topic <t> --partition <p> --offset <o>"`.

## Observability

Plan: phase 10. Spec: 15.4.

Metrics: `/actuator/prometheus` on each app's management port, every metric tagged `service`. No account id, payment id or raw URL in any label (tests check this).

| Metric | Type | Where |
|---|---|---|
| `http_server_requests_seconds{uri, status}` (route templates): request rate, latency and volume of deposits, withdrawals and blocks | histogram | wallet-api |
| `http_client_requests_seconds{uri, status}`: TransferService and beneficiary calls | histogram | transfer-executor, wallet-api |
| `dynamodb_request_seconds{operation, outcome}` | histogram | every app using ledger-core |
| `api_errors_total{route, error}` | counter | wallet-api |
| `withdrawal_end_to_end_seconds{outcome, reason}`; its `_count` is the finalized payments count | histogram | whoever writes the final status |
| `withdrawal_verification_seconds` | histogram | completion (`received_at` to `EXECUTING`) |
| `transfer_seconds{outcome}` | histogram | `EXECUTING` to final |
| `verification_step_seconds{step}`, `late_verification_results_total{step}` | histogram, counter | completion |
| `circuit_breaker_state` (0 closed, 1 half-open, 2 open) | gauge | transfer-executor |
| `stuck_executing_redriven_total` | counter | timeout-checker |
| `optimistic_lock_retries_total` | counter | every app using ledger-core |
| worker pool: see [worker-pool](libs/worker-pool/README.md#metrics) | | consumers |

No gauge counts payments by status (spec change 19): alert on `stuck_executing_redriven_total`, `accounts_held_total` and `withdrawal_end_to_end_seconds_count{outcome="needs_review"}` instead.

No separate counters where another metric already has the same data (spec 15.4): `payments_finalized_total` (the `_count` above), `deposits_total` / `deposit_processing_seconds` / `withdrawals_accepted_total` / `withdrawal_accept_seconds` / `account_blocks_total` (`http_server_requests_seconds` by route and status; it also counts idempotent replays), `transfer_attempts_total` (`http_client_requests_seconds{uri="/transfers"}`: 2xx ok, 422 failed, anything else unknown).

Tracing: OpenTelemetry over OTLP (`management.opentelemetry.tracing.export.otlp.endpoint`, default `http://localhost:4318/v1/traces`), sampling `management.tracing.sampling.probability` (1.0 locally). The trace context travels in the Kafka `traceparent` header: the bridge starts a trace per record, and every worker pool record and every acked publish continues it, so one withdrawal shows verifiers, completion, executor (with the TransferService call) and timeout checker in one trace. The HTTP request that created the withdrawal is a separate trace (no trace context on DynamoDB items). The bridge exports no span itself, so Jaeger shows the root as missing. Jaeger UI: http://localhost:16686.

Prometheus (http://localhost:9090) scrapes `/actuator/prometheus` of every app running locally on its default management port ([docker/prometheus.yml](docker/prometheus.yml)); apps that are not running show as down. Example query: `histogram_quantile(0.99, sum by (le, outcome) (rate(withdrawal_end_to_end_seconds_bucket[5m])))`.
