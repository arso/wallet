#!/bin/bash

set -eo pipefail
./gradlew clean build
docker compose up -d dynamodb
docker build -t arso/wallet-api wallet-api
docker run --rm -p 8080:8080 -p 8081:8081 \
  -e WALLET_DYNAMODB_ENDPOINT=http://host.docker.internal:8000 \
  arso/wallet-api
