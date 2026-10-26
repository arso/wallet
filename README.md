# Description

Wallet and payments system. Spec: [docs/payments-system-spec.md](docs/payments-system-spec.md).
Plan: [docs/implementation-plan.md](docs/implementation-plan.md).

## Modules

- `libs/ledger-core` - domain, money transactions, DynamoDB adapter
- `libs/events` - Kafka message records + JSON mapping
- `libs/worker-pool` - Kafka consumer pool: KEYED / UNORDERED / DELAYED dispatch, offset tracking, failure policy, account holds
- `wallet-api` - REST API
- `bridge` - DynamoDB stream to Kafka (`ledger-events`, `withdraw-requests`)

## Build and test

`./gradlew clean build` (needs Docker for Testcontainers).

## Run locally

`docker compose up -d` starts DynamoDB Local and Kafka. Then `./gradlew :wallet-api:bootRun`.

Bridge (local runner on the DynamoDB Local stream): `./gradlew :bridge:run`.

Recover a consumer hold (after the fix and the release), from its `hold_ref = <topic>/<partition>/<offset>`:
`./gradlew :libs:worker-pool:run --args="--topic <t> --partition <p> --offset <o>"`.
