#!/bin/bash

set -eo pipefail
./gradlew clean build
docker build -t arso/wallet .
docker run --rm -p 8080:8080 -p 8081:8081 arso/wallet
