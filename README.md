# Description

This project is a showcase of a wallet built on a single DynamoDB table for high-volume deposits and withdrawals. Every balance change is stored as an append-only ledger event, following an event sourcing model, and the DynamoDB stream feeds those events to Kafka. Withdrawals run as an asynchronous saga through verification, execution and timeout services. Each part scales horizontally. It is not a real payment system: the external actors (beneficiary lookup, verifiers, AML and the transfer service) are mocked. More detail is in the [architecture](docs/ARCHITECTURE.md), the [spec](docs/payments-system-spec.md) and the [implementation plan](docs/implementation-plan.md).

The project was also an exercise in solving a complex distributed problem effectively in a very short time (about 18 hours), by one engineer working with Claude, while (hopefully 🤞) keeping the code high quality, readable and well tested.

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
- `e2e` - user journeys on the full stack in Docker, REST only ([details](e2e/README.md))

## Build and test

`./gradlew clean build` (needs Docker for Testcontainers). End-to-end tests are separate and slow: `make e2e` (`./gradlew :e2e:e2eTest`).

## Run locally

`make up-all` starts DynamoDB Local, Kafka, Jaeger and Prometheus (`make up-deps`), builds the jars and starts every app in the background: wallet-api first (it creates the table), then the bridge (local runner on the DynamoDB Local stream), mocks, completion-service, transfer-executor and timeout-checker. Logs and pids in `.run/`. `make down-all` stops everything, `make help` lists the other targets (`status`, `logs-<app>`, `restart-apps`, `run-<app>` for one app in the foreground).

Recover a consumer hold (after the fix and the release), from its `hold_ref = <topic>/<partition>/<offset>`:
`./gradlew :libs:worker-pool:run --args="--topic <t> --partition <p> --offset <o>"`.

## Observability

Plan: phase 10. Spec: 15.4.

Metrics: `/actuator/prometheus` on each app's management port, every metric tagged `service`. No account id, payment id or raw URL in any label (tests check this).

[docs/OBSERVABILITY.md](docs/OBSERVABILITY.md):

- [Metrics](docs/OBSERVABILITY.md#metrics) (list)
- [Graph the queries in Prometheus](docs/OBSERVABILITY.md#graph-the-queries-in-prometheus) (with a traffic script)
- Health graphs: [API errors and latency](docs/OBSERVABILITY.md#1-api-errors-and-latency), [business rejections](docs/OBSERVABILITY.md#2-business-rejections-from-the-api), [withdrawal outcomes](docs/OBSERVABILITY.md#3-withdrawal-outcomes), [withdrawal time](docs/OBSERVABILITY.md#4-withdrawal-time-and-the-slow-stage), [Kafka pipeline](docs/OBSERVABILITY.md#5-kafka-pipeline-worker-pools), [DynamoDB and TransferService](docs/OBSERVABILITY.md#6-dynamodb-and-transferservice)
- [Traces in Jaeger](docs/OBSERVABILITY.md#traces-in-jaeger) (find traces by payment id or account id)
