# e2e

End-to-end tests (plan phase 11): the spec 13 scenarios that cross services, on the full stack, through the REST API only. Scenarios inside one service are covered by its module tests.

`make e2e` (`./gradlew :e2e:e2eTest`, not part of `build`) builds the boot jars and the bridge, builds images from the module Dockerfiles and starts everything with Testcontainers on one Docker network: DynamoDB Local, Kafka, wallet-api, bridge (local stream runner), mocks (verifiers, AML, beneficiary, TransferService), completion-service, transfer-executor and timeout-checker. The verification timeout is short (15s).

After each scenario the invariants from spec 12 are checked on the accounts it used: seq gap-free from 1, replaying the ledger gives every `balance_after` and the account balance, balance never negative, every payment final with exactly the matching ledger entry (`NEEDS_REVIEW`: none, so no refund), ledger entries never change.

Mock rules used by the scenarios: [mocks](../mocks/README.md).
