# mocks

Stand-ins for the external services (spec 2, "Out of scope"). One Spring Boot app, port 8090. Never writes to DynamoDB.

Behaviour is chosen by ids, so tests stay deterministic and can run in parallel.

| Mock | Interface | Rule |
|---|---|---|
| Beneficiary | `GET /beneficiaries/{id}` | any id exists, except ids starting `missing` (404). Data derived from the id. |
| TransferService | `POST /transfers`, `Idempotency-Key: <payment_id>`, body `payment_id, account_id, amount, currency, beneficiary_id` | beneficiary `b-reject*`: 422 `transfer_rejected`. `b-timeout*`: answers after `mocks.transfer.timeout-delay` (caller times out, outcome unknown). Else 200 `{"external_tx_id"}`. Same key, same result. |
| Verifiers | `withdraw-requests` to `verification-results`, one consumer group per step (`verifier-<step>`) | after a random latency. Beneficiary containing `deny-<step>`: rejected. `silent-<step>`: never answers (timeout). Else allowed. |
| AML | `ledger-events` to wallet-api `/internal/accounts/{id}/block\|unblock` | deposit >= `mocks.aml.block-threshold`: block. Deposit with `external_tx_id` starting `aml-unblock`: unblock. Refused with `account_on_hold`: kept and applied on `account_released` (in memory). |

Run: `./gradlew :mocks:bootRun` (needs Kafka; AML needs wallet-api on 8080). Turn parts off with `mocks.verifiers.enabled=false`, `mocks.aml.enabled=false`.
