# Description

Wallet and payments system. Spec: [docs/payments-system-spec.md](docs/payments-system-spec.md).
Plan: [docs/implementation-plan.md](docs/implementation-plan.md).

## Modules

- `libs/ledger-core` - domain, money transactions, DynamoDB adapter
- `libs/events` - Kafka message records + JSON mapping
- `wallet-api` - REST API
- `bridge` - DynamoDB stream to Kafka (`ledger-events`, `withdraw-requests`)

## Build and test

`./gradlew clean build` (needs Docker for Testcontainers).

## Run locally

`docker compose up -d` starts DynamoDB Local and Kafka. Then `./gradlew :wallet-api:bootRun`.

Bridge (local runner on the DynamoDB Local stream): `./gradlew :bridge:run`.
