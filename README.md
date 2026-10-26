# Description

Wallet and payments system. Spec: [docs/payments-system-spec.md](docs/payments-system-spec.md).
Plan: [docs/implementation-plan.md](docs/implementation-plan.md).

## Modules

- `libs/ledger-core` - domain, money transactions, DynamoDB adapter
- `wallet-api` - REST API

## Build and test

`./gradlew clean build` (needs Docker for Testcontainers).

## Run locally

`docker compose up -d` starts DynamoDB Local and Kafka. Then `./gradlew :wallet-api:bootRun`.
