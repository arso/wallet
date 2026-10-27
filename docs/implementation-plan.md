# Wallet & Payments System - Implementation Plan

Spec: [payments-system-spec.md](payments-system-spec.md). Section numbers below (e.g. "spec 6.2") point to it.

Status: agreed, not started.

---

## 1. Decisions

- **Web stack:** Spring MVC + virtual threads, blocking clients (spec 4, "Virtual threads notes"). Replace the current `spring-boot-starter-webflux` with `spring-boot-starter-web`.
- **Modules:** one Gradle module per app, so each app builds its own Docker image. Two shared libraries for code that must not be copied.
- **Hexagonal structure inside each module** (spec 4, "Code structure"). ArchUnit rules run in every module.
- **Spec changes agreed** (see section 2).

## 2. Spec changes

| # | Change | Spec ref | Why |
|---|---|---|---|
| 1 | Add a `DEPOSIT#<external_tx_id>` item to the deposit transaction, condition `attribute_not_exists`. A duplicate returns the original deposit. | 5.1, 6.2, 9.2 | A provider retry with a new `Idempotency-Key` would otherwise credit the same deposit twice. |
| 2 | `account_id` is a ULID. | 5.2, 9.1 | Spec does not define the account ID format. |
| 3 | Idempotency `request_hash` covers method + path (with account ID) + body. Later, with auth: key scoped per caller, `IDEMPOTENCY#<caller>#<key>`. | 5.7, 10 | Same key + same body on a different account must not return the other account's response. |
| 4 | User cancel is allowed on a blocked account. Money goes back to the frozen account. | 6.2, 14 (open question) | No money leaves the account. Refusing would only delay the same refund. |
| 5 | Drop GSI1. `GET /accounts/{id}/ledger` pages by `seq` on the main table, newest first. No `from` / `to` date filter in v1. | 5.4, 5.8, 10 | GSI1 adds ~10-12% write cost and a second copy of the ledger, only for date filtering. Rich history queries go to the future read model (spec 14). |
| 6 | Drop the `PROFILE#` item. No user link in the wallet: the user service stores the `account_id` it gets from `POST /accounts`, and AML / support ask the user service who owns an account. | 5.1, 5.3, 9.1 | Wallet owns money only, user service owns people. The item would only repeat `account_id` and `created_at` from `ACCOUNT#`. |
| 7 | Ledger `timestamp` renamed to `recorded_at`. | 5.4 | `timestamp` does not say what the time means. It is when the wallet wrote the entry, the same meaning as verification `recorded_at`. |
| 8 | `executing_at` on payment. | 5.6 | `EXECUTING` writes no ledger entry, so nothing else records when execution started. Needed for `transfer_seconds` and the safety-net age check (spec change 14). |
| 9 | Idempotency `response` split into `response_status` + `response_body`. | 5.7 | Replay the exact status code. |
| 10 | `schema_version` on every Kafka message. `event_id` (ULID) only on `verification-results` and `transfer-requests`. | 7 | Safe schema changes. Tracing of messages without a natural ID; ledger-based messages use `account_id` + `seq`. |
| 11 | `requested_at` on `withdraw-requests` (= ledger `recorded_at` of the `withdraw` entry). | 7, 8.6 | Event time for the timeout checker. The Kafka record timestamp can be overwritten by broker settings. |
| 12 | Shared lib `libs/events` with the Kafka message records. | 7 | Bridge, mocks and consumers share one definition. |
| 13 | Kafka delivery and failure policy (section 5.6): acked idempotent producers, send-wait before marking done, transient errors retried without limit, failed records put the account `ON_HOLD` (spec change 21). | 7, 8.1, 8.4 | Without it a lost publish or a dropped record leaves a payment stuck forever in `PENDING` or `EXECUTING`. |
| 14 | Timeout checker also re-drives stuck `EXECUTING` payments: if `executing_at` is older than a limit (default 15 min), it republishes `transfer-requests`. If `EXECUTING` is newer than the limit, it re-checks at `executing_at` + limit. | 8.6 | Safety net for bugs, failed records or lost messages. The executor is idempotent, so a republish is safe. No extra cost: the checker already reads every payment once. |
| 15 | Read rules (section 3.10): every `PAYMENT#` read that drives a decision is strongly consistent; completion reads nothing and evaluates the item returned by its step write. | 6.1, 8.3, 8.5, 8.6 | An eventually consistent read can see a stale `PENDING`, so the executor skips and the payment is stuck in `EXECUTING`, or completion misses its own write and the payment is refunded after the timeout. |
| 16 | Transaction outcome rules (section 3.11): only `ConditionalCheckFailed` is a business answer; conflicts and throttling are retried; validation errors are bugs. | 6.1, 6.2 | Reading `TransactionConflict` as "stop" leaves a fully allowed payment `PENDING` until it is refunded by the timeout. |
| 17 | Idempotency lookup first (section 3.8): consistent read of `IDEMPOTENCY#` before any check or external call, and again before returning any error; `IDEMPOTENCY#` wins when several transaction conditions fail (section 3.11); final business errors (400, 404, 409, 422) are stored too; expired-but-not-deleted items count as absent. | 5.7, 6.1, 10 | Otherwise a retry after the state changed (balance drained, account blocked, beneficiary down) gets 422 / 409 / 503 instead of the stored 202, and a client may retry with a new key and withdraw twice. Storing errors stops a rejected withdraw from silently succeeding on a later retry. |
| 18 | Worker pool dispatch modes (phase 5): completion keeps per-payment queues (KEYED); the transfer executor runs each record on its own virtual thread, capped by a semaphore (UNORDERED); the timeout checker processes each record at its due time (DELAYED), through the same permit pool, and gets the same failure policy. Each partition keeps a set of pause reasons. | 8.4, 8.5, 15.4 (Java 25 note) | With per-payment queues, one transfer retry (up to ~2.5 min) delays every payment queued behind it. The executor gains nothing from ordering: one message per payment, duplicates are safe through the TransferService idempotency key, the `EXECUTING` check and the `complete` condition. |
| 19 | Drop the `payments_executing`, oldest-age-in-`EXECUTING` and `payments_needs_review` gauges. Alert instead on `stuck_executing_redriven_total`, `accounts_held_total` and `withdrawal_end_to_end_seconds_count{outcome="needs_review"}`. | 15.4 | A count by status needs a table Scan (the table grows ~350 GB/day) or a GSI. The timeout checker already looks at every payment, so a payment stuck in `EXECUTING` always shows up as a re-drive. |
| 20 | Verification `details` stored on `PAYMENT#` cut to 100 characters; the full text stays in the `verification-results` message. | 5.6, 8.3 | Free text with no bound. Above ~110 characters per step `PAYMENT#` goes over 1 KB and every later write to it costs double; also removes the 400 KB item limit risk. Cut, not rejected, so a long text never puts an account on hold. |
| 21 | No DLQ topics. A record that fails permanently, or uses up its unknown-error budget, puts its account `ON_HOLD` (new `ACCOUNT#` status) with a ledger entry `account_held` that keeps `hold_ref` (topic / partition / offset); the record is then marked done. Only the internal release endpoint clears it, to a status the operator chooses. Not for the bridge: it fails the batch and stalls instead (spec change 22). | 6.2, 7, 8.1, 8.4, 10 | Kafka already keeps the record; a DLQ is a second copy plus partition and resolver setup. A hold freezes the money where it is, makes the problem visible per account, and never blocks a partition. |
| 22 | The bridge never holds an account and never skips a record: any error fails the stream batch, which is sent again until it goes through. No republish command. | 7, 8.1 | Permanent errors are near impossible in the bridge (item <= 400 KB < 1 MB Kafka limit; same mapper that wrote the item), so a hold path, its loop guard and a republish tool cost more than they save. A deploy bug stalls one shard; `IteratorAge` alerts long before the 24h retention. |

Implementation notes (no spec change, only made explicit):

- Transaction cancel handling: see section 3.11 (spec change 16).
- Resilience4j: use the core libraries with manual config if the Spring Boot starter does not support Boot 4 yet. (spec 4, "Tech stack")
- Transfer retries hold one executor permit for up to ~2.5 min (5 retries from 5s). Size the executor `max-in-flight` from TransferService capacity and this retry time, not from CPU count. (spec 8.5, 11)

## 3. DynamoDB table

Spec ref: 5, 6.

### 3.1 Table

| Setting | Value |
|---|---|
| Table name | `wallet-<env>`, e.g. `wallet-local`, `wallet-dev`, `wallet-prod`. Set via `wallet.dynamodb.table-name`. |
| Partition key | `PK` (S) |
| Sort key | `SK` (S) |
| Billing mode | On-demand (`PAY_PER_REQUEST`) |
| Stream | Enabled, `NEW_IMAGE` (bridge needs only the new item) |
| TTL attribute | `ttl` (idempotency items only) |
| Point-in-time recovery | Enabled (dev, prod) |
| Deletion protection | Enabled (prod) |
| LSIs | None (spec 5.1) |
| GSIs | None (spec change 5) |

Attribute definitions (key attributes only): `PK` S, `SK` S.

DynamoDB types used: `S` string, `N` number, `BOOL` boolean, `M` map, `L` list.

Common formats:

- IDs: ULID, 26 chars, e.g. `01JBC3Z8Q4M6T2N9X5R7K1W0HV`.
- Timestamps: UTC ISO-8601, always 3 ms digits, e.g. `2026-10-26T12:00:00.123Z`. Fixed width, so string sort = time sort.
- Money: `N`, integer minor units (cents). Java `long`.
- Currency: `S`, ISO-4217, e.g. `USD`.
- Field names: snake_case, as in the spec.
- Time field names:
  - `recorded_at`: when the wallet wrote it (ledger entries, verification results).
  - `received_at`, `requested_at`, `decided_at`, `executing_at`, `finalized_at`: when a specific business step happened.
  - `created_at`, `updated_at`: row lifecycle (account, idempotency, deposit dedup).

### 3.2 Ledger history query (no GSI)

- `Query` on the main table: `PK = <account_id> AND SK BETWEEN "LEDGER#000000000001" AND "LEDGER#<before_seq - 1>"`, `ScanIndexForward = false`, `Limit = <page size>`.
- First page: `SK begins_with "LEDGER#"`, newest first.
- API: `GET /accounts/{id}/ledger?limit=50&before_seq=<seq>`. The response holds `next_before_seq` (lowest `seq` on the page) or `null` on the last page.
- Default page size 50, max 200.
- Strongly consistent reads are possible (main table), but eventually consistent is used by default (half the read cost).
- Date filtering later: binary search on `seq`, or a read model built from `ledger-events` (spec 14).

### 3.3 Item types

| Item | PK | SK |
|---|---|---|
| Account | `<account_id>` | `ACCOUNT#` |
| Ledger entry | `<account_id>` | `LEDGER#<seq, 12 digits>` |
| Payment | `PAYMENT#<payment_id>` | `PAYMENT#` |
| Idempotency | `IDEMPOTENCY#<key>` | `IDEMPOTENCY#` |
| Deposit dedup | `DEPOSIT#<external_tx_id>` | `DEPOSIT#` |

### 3.4 Account (`ACCOUNT#`)

| Field | Type | Required | Notes |
|---|---|---|---|
| `PK` | S | yes | `<account_id>` |
| `SK` | S | yes | `ACCOUNT#` |
| `account_id` | S | yes | ULID |
| `balance` | N | yes | minor units, never < 0 |
| `currency` | S | yes | e.g. `USD` |
| `status` | S | yes | `ACTIVE` \| `BLOCKED` \| `ON_HOLD` (spec change 21) |
| `version` | N | yes | starts at 1, +1 on every change |
| `pre_hold_status` | S | no | only while `ON_HOLD`: status before the first hold (`ACTIVE` or `BLOCKED`); repeat holds do not change it |
| `hold_refs` | SS | no | only while `ON_HOLD`: every `hold_ref` of the current hold; makes a repeated hold idempotent |
| `created_at` | S | yes | timestamp |
| `updated_at` | S | yes | timestamp |

Status rules:

| From | Action | To | Who |
|---|---|---|---|
| `ACTIVE` | block | `BLOCKED` | AML (internal API) |
| `BLOCKED` | unblock | `ACTIVE` | AML (internal API) |
| `ACTIVE`, `BLOCKED` | hold | `ON_HOLD` (sets `pre_hold_status` to the old status and `hold_refs` to the first `hold_ref`) | any consumer, on a record that cannot be processed (section 5.6) |
| `ON_HOLD` | hold again | `ON_HOLD` (version bump, new `account_held` entry with the new `hold_ref`, added to `hold_refs`; `pre_hold_status` unchanged; no-op if `hold_ref` is already in `hold_refs`) | same |
| `ON_HOLD` | release (`to` required) | `ACTIVE` or `BLOCKED` | operator, `POST /internal/accounts/{id}/release` |
| `ON_HOLD` | block / unblock | refused, 409 `account_on_hold` | AML |

- `ON_HOLD` stops user operations like `BLOCKED` (API 409 `account_on_hold`); user cancel and system refunds still work; mark executing rejects with `ACCOUNT_BLOCKED`.
- Block / unblock never touch a held account, so an AML unblock cannot clear a hold. AML keeps the blocks and unblocks it could not apply (409 `account_on_hold`) and applies them when it sees `account_released`.
- Release takes a required `to` (`ACTIVE` or `BLOCKED`). It reads `pre_hold_status` from `ACCOUNT#` (same consistent read it already does; no ledger scan) and refuses `to = ACTIVE` over a `pre_hold_status` of `BLOCKED` unless the request sets `confirm = true`. Release on an account that is not `ON_HOLD` gives 409. Release removes `pre_hold_status` and `hold_refs`.
- Every `hold_ref` is in the ledger, so all failed records of an account can be found for replay.

### 3.5 Profile

Dropped (spec change 6). Add back when there is real static account data.

### 3.6 Ledger entry (`LEDGER#<seq>`)

| Field | Type | Required | Notes |
|---|---|---|---|
| `PK` | S | yes | `<account_id>` |
| `SK` | S | yes | `LEDGER#000000000001` |
| `account_id` | S | yes | |
| `seq` | N | yes | same value as in SK, gap-free from 1 |
| `event_type` | S | yes | see table below |
| `recorded_at` | S | yes | server time when the wallet wrote the entry (spec name: `timestamp`) |
| `balance_after` | N | yes | minor units |
| `actor` | S | yes | `user` \| `completion-service` \| `transfer-executor` \| `timeout-checker` \| `bridge` \| `aml` \| `internal` |
| `reason` | S | no | free text or reject reason |
| `payment_id` | S | no | withdrawal-related events |
| `amount` | N | no | events that move money |
| `currency` | S | no | events that move money |
| `external_tx_id` | S | no | `deposit`, `withdraw_completed` |
| `payer` | M | no | `deposit` only, see counterparty |
| `beneficiary` | M | no | `withdraw` only, see counterparty |
| `verifier` | S | no | `reject_withdraw` with `VERIFICATION_REJECTED` |
| `hold_ref` | S | no | `account_held` only: failed record, `<topic>/<partition>/<offset>`. |
| `prior_status` | S | no | `account_held` only: status before the hold (`ACTIVE`, `BLOCKED`, or `ON_HOLD` for a repeat hold) |
| `released_to` | S | no | `account_released` only: `ACTIVE` or `BLOCKED` |

Fields per event type:

| `event_type` | Balance effect | Extra fields |
|---|---|---|
| `account_created` | set to 0 | - |
| `deposit` | + amount | `amount`, `currency`, `external_tx_id`, `payer` |
| `withdraw` | - amount | `amount`, `currency`, `payment_id`, `beneficiary` |
| `withdraw_completed` | none | `payment_id`, `external_tx_id` |
| `user_cancel_withdraw` | + amount | `amount`, `currency`, `payment_id` |
| `reject_withdraw` | + amount | `amount`, `currency`, `payment_id`, `reason`, `verifier` (if relevant) |
| `account_blocked` | none | `reason` |
| `account_unblocked` | none | `reason` |
| `account_held` | none | `reason` (error class), `hold_ref`, `prior_status` |
| `account_released` | none | `reason`, `released_to` |

`reject_withdraw` reasons (`reason`): `VERIFICATION_REJECTED`, `VERIFICATION_TIMEOUT`, `ACCOUNT_BLOCKED`, `EXECUTION_FAILED`.

Counterparty map (`payer` / `beneficiary`, type M):

| Field | Type | Required | Notes |
|---|---|---|---|
| `payer_id` / `beneficiary_id` | S | yes | external ID |
| `name` | S | yes | |
| `account_masked` | S | yes | e.g. `****1234` |
| `account_fingerprint` | S | yes | HMAC from the external service |
| `country` | S | yes | ISO-3166 alpha-2 |

Full account numbers are never stored (spec 5.5).

### 3.7 Payment (`PAYMENT#<payment_id>`)

| Field | Type | Required | Notes |
|---|---|---|---|
| `PK` | S | yes | `PAYMENT#<payment_id>` |
| `SK` | S | yes | `PAYMENT#` |
| `payment_id` | S | yes | ULID |
| `account_id` | S | yes | |
| `amount` | N | yes | minor units |
| `currency` | S | yes | |
| `beneficiary_id` | S | yes | |
| `status` | S | yes | `PENDING` \| `EXECUTING` \| `COMPLETED` \| `REJECTED` \| `CANCELLED` \| `NEEDS_REVIEW` |
| `received_at` | S | yes | HTTP request arrival |
| `executing_at` | S | no | set on move to `EXECUTING` (spec change 8) |
| `finalized_at` | S | no | set on final status |
| `reject_reason` | S | no | when `REJECTED` |
| `external_tx_id` | S | no | from TransferService |
| `required_steps` | L of S | yes | copied from config at creation, never changed. List, not set: a set cannot be empty. |
| `verifications` | M | yes | created as `{}`. Key = step name, value = verification map |

Verification map (value in `verifications`, type M):

| Field | Type | Required | Notes |
|---|---|---|---|
| `allowed` | BOOL | yes | |
| `details` | S | no | cut to 100 characters (spec change 20) |
| `decided_at` | S | yes | verifier time |
| `recorded_at` | S | yes | completion service time |
| `late` | BOOL | no | only present, as `true`, if recorded after the payment was final |

### 3.8 Idempotency (`IDEMPOTENCY#<key>`)

| Field | Type | Required | Notes |
|---|---|---|---|
| `PK` | S | yes | `IDEMPOTENCY#<key>` |
| `SK` | S | yes | `IDEMPOTENCY#` |
| `request_hash` | S | yes | SHA-256 hex of method + path + canonical JSON body (sorted keys, no whitespace) |
| `response_status` | N | yes | HTTP status |
| `response_body` | S | yes | JSON |
| `created_at` | S | yes | timestamp |
| `ttl` | N | yes | epoch seconds, `created_at` + 24h |

- Spec has one `response` field. Split in two so the status is kept too (spec change 9).
- Success responses are written in the same transaction as the operation.
- Final business errors (400, 404, 409, 422) are stored too, with a separate `PutItem` (spec change 17). 503 and other 5xx are never stored, so the client can retry.
- Put condition (success and error): `attribute_not_exists(PK) OR ttl < :now`. An expired item may still exist (TTL deletes late), and must not block a reused key.
- If an error put fails because a concurrent request already stored a response, the error is not stored. The client gets the stored response on its next retry.

Request flow for every POST (spec change 17):

1. Require `Idempotency-Key`. Compute `request_hash`.
2. Consistent `GetItem` on `IDEMPOTENCY#<key>`:
   - found, same hash: return stored `response_status` + `response_body`. No checks, no external calls.
   - found, different hash: 409.
   - not found, or `ttl < now`: continue.
3. Run the operation (checks, beneficiary lookup, transaction with the `IDEMPOTENCY#` put).
   - 3a. Before returning any error from step 3 (400, 404, 409, 422 or 503, also after the lock retry budget is used up): consistent `GetItem` on `IDEMPOTENCY#<key>` again. Found, same hash: return the stored response. Found, different hash: 409. Not found: return the error, and store it if final.
4. Transaction cancelled by the `IDEMPOTENCY#` condition (a concurrent request with the same key won): take the old item from the cancel reason (3.11). Same hash: return its stored response. Different hash: 409.

Two concurrent requests with the same key: both pass step 2, one transaction commits, the other returns the winner's response. If the loser fails an in-code check first (balance drained or account blocked by the winner, beneficiary down), step 3a finds the winner's item and replays it. If the in-code checks pass, the transaction hits the `IDEMPOTENCY#` condition and step 4 applies. No extra lookup on each lock retry. The beneficiary may be called twice; harmless (read only). The extra read costs 1 RRU on error paths only.

### 3.9 Deposit dedup (`DEPOSIT#<external_tx_id>`)

| Field | Type | Required | Notes |
|---|---|---|---|
| `PK` | S | yes | `DEPOSIT#<external_tx_id>` |
| `SK` | S | yes | `DEPOSIT#` |
| `external_tx_id` | S | yes | |
| `account_id` | S | yes | |
| `seq` | N | yes | ledger entry of the deposit |
| `created_at` | S | yes | timestamp |

- No TTL. Kept forever.
- Duplicate on the same account: return the original deposit (read ledger by `account_id` + `seq`).
- Duplicate on a different account: `409`.
- Assumes one payment provider. With more providers, the key becomes `DEPOSIT#<provider>#<external_tx_id>`.

### 3.10 Read rules

Spec change 15. Every read that drives a decision is strongly consistent, or is replaced by the result of a write.

| Who | Item | How |
|---|---|---|
| API, all writers | `ACCOUNT#` | `GetItem`, `ConsistentRead=true` (spec 6.1) |
| Completion | `PAYMENT#` | no read. The step write returns the item (see below). |
| Transfer executor | `PAYMENT#` | `GetItem`, `ConsistentRead=true`, before calling TransferService |
| Timeout checker | `PAYMENT#` | `GetItem`, `ConsistentRead=true`. Most payments are final at this point, so one cheap read beats an account read + transaction attempt per payment. |
| Transaction cancelled by a condition | any | `ReturnValuesOnConditionCheckFailure=ALL_OLD` on the item, read from the cancellation reason |
| Ledger history API | `LEDGER#` | eventually consistent (3.2) |

Completion step write:

1. `UpdateItem` `SET verifications.<step> = :v`, condition `attribute_not_exists(verifications.<step>) AND status IN (PENDING, EXECUTING)`, `ReturnValues=ALL_NEW`, `ReturnValuesOnConditionCheckFailure=ALL_OLD`.
2. Success: evaluate the returned item (reject / mark executing / republish).
3. Condition failed, step already present: duplicate. Evaluate the returned old item anyway (fixes the crash between step write and `EXECUTING`).
4. Condition failed, payment final and step absent: write the step again with `late: true` (condition `attribute_not_exists(verifications.<step>)`). No other action.

Cost and side effects:

- Consistent read = 1 RRU per 4 KB instead of 0.5. `ReturnValues` costs no read capacity.
- Per withdrawal: 2 consistent reads (executor, timeout checker) instead of ~5 eventually consistent reads. Net fewer read units.
- Consistent reads can fail with HTTP 500 during a storage-node issue. Handled as a transient error (5.6).
- Consistent only within one region (relevant for spec 16 only).

### 3.11 Transaction outcome rules

Spec change 16. One classifier in `ledger-core` reads all per-item cancel reasons of a `TransactionCanceledException` (and other SDK errors) and returns one result:

| Result | When | Action |
|---|---|---|
| `CONDITION_FAILED` (item, old item) | any reason is `ConditionalCheckFailed` | decided per item (table below); with several failed items, the first in the precedence list decides. Wins over other reasons: it was checked against committed state. |
| `RETRY` | `TransactionConflict`, `ThrottlingError`, `ProvisionedThroughputExceeded`; exceptions `InternalServerError`, `RequestLimitExceeded`, `TransactionInProgressException`, client timeout | back off and retry |
| `FAIL` | `ValidationError`, `ItemCollectionSizeLimitExceeded`, `IdempotentParameterMismatch` | bug: permanent error (5.6) |

- `None` reasons are ignored.
- Precedence when several items fail their condition (each comes with its old item):
  1. `IDEMPOTENCY#`: replay the stored response, or 409 on a different hash.
  2. `DEPOSIT#`: original deposit, or 409 on another account.
  3. `PAYMENT#`: stop, or the reject flow.
  4. `ACCOUNT#` business answer: blocked or insufficient funds.
  5. `ACCOUNT#` version or `LEDGER#` seq conflict: retry.
- Every condition in a transaction sets `ReturnValuesOnConditionCheckFailure=ALL_OLD`, so the current state of the failed item comes back with the error.
- A `RETRY` of the exact same request reuses the same `ClientRequestToken`. If the first attempt committed (e.g. client timeout), DynamoDB returns success instead of applying it twice.

`CONDITION_FAILED` per item:

| Failed item | Old item shows | Meaning | Action |
|---|---|---|---|
| `ACCOUNT#` | `version` differs | optimistic lock conflict | re-read and retry (lock retry budget) |
| `ACCOUNT#` | `status = BLOCKED` or `ON_HOLD` | account blocked or held | API: 409 (`account_blocked` / `account_on_hold`). Mark executing: reject `ACCOUNT_BLOCKED`. |
| `ACCOUNT#` | `balance` < amount | insufficient funds | API: 422 |
| `ACCOUNT#` (hold) | `hold_ref` already in `hold_refs` | same hold applied before | no-op, mark done |
| `ACCOUNT#` (release) | `status` not `ON_HOLD` | nothing to release | API: 409 |
| `LEDGER#<seq>` | exists | another writer took the seq | same as a version conflict |
| `PAYMENT#` | status not as expected | payment moved on | stop (API cancel: 409) |
| `PAYMENT#` (mark executing) | a required step not `allowed` | a reject exists | run the reject flow if `PENDING` (spec 9.8) |
| `IDEMPOTENCY#` | exists | same key in flight or done | replay the stored response |
| `DEPOSIT#` | exists | duplicate deposit | same account: original deposit. Other account: 409. |

Retry budgets:

- API: `RETRY` and version conflicts share one budget (default 3, random backoff), then 503 (spec 6.1).
- Consumers: an exhausted budget is a transient error (5.6). The worker keeps retrying, the offset stays uncommitted. Nothing is dropped or wrongly decided.

## 4. Domain model (Java)

Spec ref: 4 ("Code structure"), 5.

Plain Java records, enums and sealed interfaces in `ledger-core`. No Spring, AWS, Kafka or Jackson imports. Optional fields are nullable; accessors for them return `Optional`.

### 4.1 Value types

| Type | Fields | Notes |
|---|---|---|
| `AccountId` | `String value` | ULID, validated |
| `PaymentId` | `String value` | ULID, validated |
| `Money` | `long amountMinor`, `Currency currency` | `amountMinor` > 0 for operations, >= 0 for balances |
| `Currency` | `String code` | ISO-4217, default `USD` |
| `Counterparty` | `String id`, `String name`, `String accountMasked`, `String accountFingerprint`, `String country` | payer or beneficiary snapshot |

### 4.2 Enums

| Enum | Values |
|---|---|
| `AccountStatus` | `ACTIVE`, `BLOCKED`, `ON_HOLD` |
| `PaymentStatus` | `PENDING`, `EXECUTING`, `COMPLETED`, `REJECTED`, `CANCELLED`, `NEEDS_REVIEW` |
| `RejectReason` | `VERIFICATION_REJECTED`, `VERIFICATION_TIMEOUT`, `ACCOUNT_BLOCKED`, `EXECUTION_FAILED` |
| `Actor` | `USER`, `COMPLETION_SERVICE`, `TRANSFER_EXECUTOR`, `TIMEOUT_CHECKER`, `BRIDGE`, `AML`, `INTERNAL` |
| `LedgerEventType` | `ACCOUNT_CREATED`, `DEPOSIT`, `WITHDRAW`, `WITHDRAW_COMPLETED`, `USER_CANCEL_WITHDRAW`, `REJECT_WITHDRAW`, `ACCOUNT_BLOCKED`, `ACCOUNT_UNBLOCKED`, `ACCOUNT_HELD`, `ACCOUNT_RELEASED` |

Stored and sent in the spec's spelling: `event_type` as snake_case (`reject_withdraw`), `actor` as kebab-case (`completion-service`), all others upper case (`PENDING`).

### 4.3 Aggregates

**`Account`**

| Field | Java type |
|---|---|
| `id` | `AccountId` |
| `balance` | `Money` |
| `status` | `AccountStatus` |
| `version` | `long` |
| `preHoldStatus` | `AccountStatus` (nullable; set only while `ON_HOLD`) |
| `holdRefs` | `Set<String>` (immutable; empty unless `ON_HOLD`) |
| `createdAt`, `updatedAt` | `Instant` |

Rules: `deposit(Money)`, `withdraw(Money)`, `refund(Money)`, `block()`, `unblock()`, `hold(reason, holdRef)`, `release(reason, to, confirm)`. `release` refuses `to = ACTIVE` over a `preHoldStatus` of `BLOCKED` unless `confirm` is true. Each returns the new `Account` (version + 1) and one `LedgerEntry`. User operations fail on `BLOCKED` and `ON_HOLD`; refunds and user cancel work. Status rules from section 3.4. Withdraw fails if balance < amount.

**`Payment`**

| Field | Java type |
|---|---|
| `id` | `PaymentId` |
| `accountId` | `AccountId` |
| `amount` | `Money` |
| `beneficiaryId` | `String` |
| `status` | `PaymentStatus` |
| `receivedAt` | `Instant` |
| `executingAt`, `finalizedAt` | `Instant` (nullable) |
| `rejectReason` | `RejectReason` (nullable) |
| `externalTxId` | `String` (nullable) |
| `requiredSteps` | `List<String>` (immutable) |
| `verifications` | `Map<String, Verification>` (immutable) |

Rules: `cancel()`, `reject(reason)`, `markExecuting()`, `complete(externalTxId)`, `needsReview()`, `recordVerification(step, Verification)`, `allRequiredAllowed()`, `anyRejected()`. Invalid transitions throw a domain exception (spec 5.9).

**`Verification`**

| Field | Java type |
|---|---|
| `allowed` | `boolean` |
| `details` | `String` (nullable) |
| `decidedAt`, `recordedAt` | `Instant` |
| `late` | `boolean` (stored only when `true`) |

### 4.4 Ledger entries

`sealed interface LedgerEntry` with common fields:

| Field | Java type |
|---|---|
| `accountId` | `AccountId` |
| `seq` | `long` |
| `recordedAt` | `Instant` |
| `balanceAfter` | `long` (minor units) |
| `actor` | `Actor` |
| `reason` | `String` (nullable) |

One record per event type:

| Record | Extra fields |
|---|---|
| `AccountCreated` | - |
| `Deposited` | `Money amount`, `String externalTxId`, `Counterparty payer` |
| `Withdrawn` | `Money amount`, `PaymentId paymentId`, `Counterparty beneficiary` |
| `WithdrawCompleted` | `PaymentId paymentId`, `String externalTxId` |
| `UserCancelledWithdraw` | `Money amount`, `PaymentId paymentId` |
| `WithdrawRejected` | `Money amount`, `PaymentId paymentId`, `RejectReason rejectReason`, `String verifier` (nullable) |
| `AccountBlocked` | - (uses `reason`) |
| `AccountUnblocked` | - (uses `reason`) |
| `AccountHeld` | `String holdRef`, `AccountStatus priorStatus` (uses `reason`) |
| `AccountReleased` | `AccountStatus releasedTo` (uses `reason`) |

`switch` over the sealed interface is exhaustive, so a new event type breaks compilation wherever it is not handled.

## 5. Kafka events

Spec ref: 7, 8.

### 5.1 Common rules

- Format: JSON, UTF-8, snake_case field names. Money as integer minor units. Timestamps as in 3.1.
- Every message has `schema_version` (integer, starts at 1). Messages not built from a ledger entry (`verification-results`, `transfer-requests`) also have `event_id` (ULID, for tracing and logs). Ledger-based messages are identified by `account_id` + `seq`.
- Headers: `traceparent` / `tracestate` (W3C tracing), `content-type: application/json`, `event_type` (on `ledger-events` only, for filtering without parsing).
- Consumers ignore unknown fields. New fields are always optional. A breaking change bumps `schema_version`, and consumers support the old and new version during rollout.
- Delivery is at-least-once. Consumers dedup by state (conditional writes), not by message IDs.
- Message records live in a new shared lib `libs/events` (plain records + Jackson mapping), used by bridge, mocks and consumers (spec change 12).

### 5.2 `ledger-events` (key: `account_id`)

Producer: bridge. Consumers: AML mock, future read models. One message per ledger entry.

| Field | Type | Required | Notes |
|---|---|---|---|
| `schema_version` | int | yes | `1` |
| `account_id` | string | yes | |
| `seq` | long | yes | |
| `event_type` | string | yes | spec 5.5 values plus `account_held`, `account_released` (spec change 21) |
| `recorded_at` | string | yes | ledger entry `recorded_at` |
| `balance_after` | long | yes | |
| `actor` | string | yes | |
| `reason` | string | no | |
| `payment_id` | string | no | |
| `amount` | long | no | |
| `currency` | string | no | |
| `external_tx_id` | string | no | |
| `payer` | object | no | counterparty, see 3.6 |
| `beneficiary` | object | no | counterparty, see 3.6 |
| `verifier` | string | no | |
| `hold_ref` | string | no | `account_held` only |
| `prior_status` | string | no | `account_held` only |
| `released_to` | string | no | `account_released` only |

Example:

```json
{
  "schema_version": 1,
  "account_id": "01JBC3Z8Q4M6T2N9X5R7K1W0HV",
  "seq": 2,
  "event_type": "deposit",
  "recorded_at": "2026-10-26T12:00:00.123Z",
  "balance_after": 10000,
  "actor": "user",
  "amount": 10000,
  "currency": "USD",
  "external_tx_id": "prov-778812",
  "payer": {
    "payer_id": "p-123",
    "name": "John Smith",
    "account_masked": "****1234",
    "account_fingerprint": "hmac:9f2c...",
    "country": "PL"
  }
}
```

### 5.3 `withdraw-requests` (key: `payment_id`)

Producer: bridge (from `withdraw` ledger entries). Consumers: verifiers, timeout checker.

| Field | Type | Required | Notes |
|---|---|---|---|
| `schema_version` | int | yes | `1` |
| `payment_id` | string | yes | |
| `account_id` | string | yes | |
| `seq` | long | yes | |
| `amount` | long | yes | |
| `currency` | string | yes | |
| `beneficiary` | object | yes | counterparty snapshot |
| `requested_at` | string | yes | ledger entry `recorded_at`. Timeout checker uses it as event time. |

Note: `required_steps` is not in the message. The bridge sees only the ledger item, and verifiers use their own config. If config changes mid-flight, a step the payment needs may never be sent. The payment then times out. This is safe but slow (spec 13, "config change mid-flight").

### 5.4 `verification-results` (key: `payment_id`)

Producer: verifier mocks. Consumer: completion service.

| Field | Type | Required | Notes |
|---|---|---|---|
| `event_id` | string | yes | ULID, new per message |
| `schema_version` | int | yes | `1` |
| `payment_id` | string | yes | |
| `step` | string | yes | e.g. `aml` |
| `allowed` | boolean | yes | |
| `details` | string | no | free text, full length; completion stores only the first 100 characters (spec change 20) |
| `decided_at` | string | yes | verifier time |

Example:

```json
{
  "event_id": "01JBC40M2S8X3V5Q7T9Y1Z3B6D",
  "schema_version": 1,
  "payment_id": "01JBC3ZK7D5E2H8J4M6N9P1R3S",
  "step": "aml",
  "allowed": false,
  "details": "sanctions list match",
  "decided_at": "2026-10-26T12:00:03.540Z"
}
```

### 5.5 `transfer-requests` (key: `payment_id`)

Producer: completion service. Consumer: transfer executor.

| Field | Type | Required | Notes |
|---|---|---|---|
| `event_id` | string | yes | ULID, new per message (duplicates on redelivery are fine) |
| `schema_version` | int | yes | `1` |
| `payment_id` | string | yes | also the TransferService idempotency key |
| `account_id` | string | yes | |
| `amount` | long | yes | |
| `currency` | string | yes | |
| `beneficiary_id` | string | yes | executor never needs full bank details; TransferService resolves them |

The executor always re-reads `PAYMENT#` and trusts its state, not the message (spec 8.5).

### 5.6 Delivery and failure policy

Spec change 13. Goal: no message that moves a payment forward is ever lost or silently dropped.

**Producers** (bridge, completion, verifier mocks, timeout checker):

- `acks=all`, `enable.idempotence=true`.
- Topics: replication factor 3, `min.insync.replicas=2`.
- `compression.type=zstd`, `linger.ms=5`, `batch.size=64KB`. Brokers keep `compression.type=producer` (no recompression). JSON batches compress ~4x; saves ~0.8 TB per retained day on `ledger-events` at RF3.
- Consumers: wait for the send result (`send().get()`) before marking the input record done. Blocking is fine on virtual threads; `linger.ms` adds ~5 ms.
- Completion: if it crashes after the `EXECUTING` commit and before the publish, the verification result is redelivered, the payment is already `EXECUTING`, and completion republishes (spec 8.3). No extra component needed.

**No DLQ** (spec change 21). Kafka already keeps every record for its retention, so a second copy in a DLQ topic adds nothing. A record that cannot be processed puts its account `ON_HOLD` instead (section 3.4), and the record is marked done. The failed record stays in Kafka; the `account_held` ledger entry holds its `hold_ref` for a manual replay. Kafka retention: at least 7 days (time to fix and replay).

**Bridge:**

Fail and alert (spec change 22): the bridge never skips a record and never holds an account.

- Send every record of a stream batch async, then `flush()`, check all results, and only then return success (the shard checkpoint moves). No per-record `send().get()`, which would cap throughput per shard.
- Any error (Kafka not acked, a ledger item that cannot be mapped) fails the whole batch. The batch is sent again until it goes through (duplicates are safe: idempotent producer, idempotent consumers). A mapping bug stalls its shard until a fixed version is deployed.
- Permanent errors are near impossible here: a DynamoDB item is at most 400 KB, below the 1 MB Kafka default (`max.request.size` and broker `message.max.bytes` stay >= 1 MB), and the mapper is the same code that wrote the item. What is left is a deploy bug: roll back or fix within the 24h Streams retention.
- Lambda (later): handler `BridgeLambdaHandler`. Event source mapping `FilterCriteria` on `eventName = INSERT` and `dynamodb.Keys.SK.S` prefix `LEDGER#` (~80% fewer invocations; the in-class filter stays as a second check). `MaximumRecordAgeInSeconds = -1` and `MaximumRetryAttempts = -1`, so no record is ever dropped; no `BisectBatchOnFunctionError`, no on-failure destination. Alert on `IteratorAge` (e.g. > 5 min) and on function errors, well before the 24h Streams retention.
- Local: `StreamRunner` polls the DynamoDB Local stream and calls the same `Bridge`; it moves the shard iterator only after a batch is published. Not for production.

**Consumers** (`worker-pool`: completion, transfer executor and timeout checker; every record ends in one of four ways):

| Outcome | Examples | Action |
|---|---|---|
| Business answer | condition failed, payment already final, duplicate | mark done |
| Known transient error | any AWS SDK error except validation and condition failures (throttling, `TransactionConflict`, timeouts, `AccessDenied`, `ExpiredToken`, `UnrecognizedClient`, `ResourceNotFound`, credential provider errors), lock retries exhausted, Kafka `RetriableException` and other non-fatal Kafka errors, failed publish, circuit open | retry in the worker, capped exponential backoff + jitter, no attempt limit. Offset stays uncommitted. |
| Permanent error | cannot deserialize, unknown `schema_version`, invalid fields | put the account `ON_HOLD`, then mark done |
| Unknown error | any other exception, e.g. `NullPointerException` or a mapping bug | retry with backoff up to a budget (default 10 attempts or 5 min, whichever first), then put the account `ON_HOLD`, then mark done. Frees the worker / permit and lets the offset move on. |

- Known transient errors retry forever on purpose: a dropped money message is worse than lag. An outage (including expired credentials or a wrong table name) shows up as lag, never as held accounts. Unknown errors get a budget, so a bug in one record cannot block a worker queue or hold a permit forever.
- The error classifier lives in `worker-pool` and classifies by exception family, not by a short list. Each app may add its own permanent and transient types. Only non-infrastructure exceptions are unknown.
- Hold flow (in `ledger-core`): reads raw attributes only, never through the domain mapper (the mapper may be what failed, or an older instance may not know `ON_HOLD`). Find the account: `ledger-events` carries `account_id`; other topics are keyed by `payment_id`, so one consistent `GetItem` on `PAYMENT#` with `ProjectionExpression = account_id`. Then read `ACCOUNT#` (`status`, `balance`, `version` only) and write `hold(account_id, reason = error class, hold_ref = topic/partition/offset)`. If a read or the hold write fails with an infrastructure error, it is transient and retried.
- Key cannot be read at all, `PAYMENT#` not found, or `ACCOUNT#` not found (no account to hold): log, metric `unattributable_records_total`, alert, mark done. The record stays in Kafka.
- Every hold attempt ends in exactly one of three ways: held; unattributable (above); or an infrastructure error (transient, retried). Nothing else can keep the record open.
- Repeat with the same `hold_ref` (a resent bridge batch, or a redelivery after a crash between the hold write and mark done) is a no-op: `ACCOUNT#` keeps every `hold_ref` of the current hold in the set `hold_refs`, and the hold condition is `attribute_not_exists(hold_refs) OR NOT contains(hold_refs, :ref)`. Release removes the set.
- Rollout: deploy readers that accept `ON_HOLD` (and the new ledger event types) in every service before any service can write a hold.
- `ExponentialBackOff` (Spring core) with `maxElapsedTime` for the budget. No `DefaultErrorHandler` (it needs listener containers).
- What a failed record leaves behind:

| Service | Payment left in | Recovery |
|---|---|---|
| completion | `PENDING`, account `ON_HOLD`; `EXECUTING` only if an unknown error came after mark executing committed | `PENDING`: timeout checker rejects and refunds at 1h (system refunds work on held accounts). `EXECUTING`: timeout checker re-drives it, as for the executor. |
| transfer executor | `EXECUTING`, account `ON_HOLD` | timeout checker re-drives it (safety net below); never refunded automatically |
| timeout checker | `PENDING` or `EXECUTING`, account `ON_HOLD` | operator: fix, release the account, then replay from `hold_ref` (replay command, phase 5) |
| bridge | nothing is skipped; the shard stalls, so later entries of every account on that shard wait | fix or roll back within 24h; the stalled batch is then published and the shard catches up |

- Recovery order is always: fix the bug, release the account, then replay / republish. While the account is `ON_HOLD`, mark executing rejects with `ACCOUNT_BLOCKED`, so a replay before release only ends in a refund.
- A replayed or republished withdraw keeps its original `requested_at`. If recovery comes more than the timeout (1h) after the withdraw, the timeout checker rejects it at once with `VERIFICATION_TIMEOUT` and refunds it. This is intended: a late recovery ends `REJECTED` with the money back, and the customer withdraws again. Inside the timeout, the payment finishes normally.
- Money is held, not lost: the account is frozen and visible, and only a human releases it. A deploy bug that hits every record puts many accounts on hold: at the budget pace for unknown errors, but as fast as records are consumed for permanent errors (for example a DynamoDB `ValidationError` from a bad expression). The alert fires on the first one; roll back, release, replay. Accepted risk: Testcontainers tests catch bad expressions, and the readers-first rollout prevents an unknown `schema_version`.
- Transfer executor: if `complete` fails after TransferService returned ok, it is a transient error and is retried. It never leads to `NEEDS_REVIEW`.

**Safety net** (spec change 14): the timeout checker reads every payment once at `requested_at + timeout`:

| Payment state | Action |
|---|---|
| `PENDING` | reject `VERIFICATION_TIMEOUT` (spec 8.6), mark done |
| `EXECUTING`, `executing_at` older than `wallet.timeout.executing-stuck-after` (default 15 min) | republish `transfer-requests` + metric `stuck_executing_redriven_total`, mark done |
| `EXECUTING`, newer | re-check at `executing_at` + limit (in-memory timer). The record stays not done until then, so the offset tracker holds the commit; after a crash it is redelivered and checked again. |
| final | mark done |

- One republish per payment. A payment still stuck after that is covered by alerts and the later reconciliation job.
- A waiting re-check does not block later records, only the commit. Commits on that partition can lag up to the limit (15 min); after a crash, up to 15 min of records are re-read (one consistent read each). Reject and republish are safe to repeat.
- A long circuit-open period can trigger a republish. This is harmless: the executor pauses on the open circuit like for any other message.

**Metrics and alerts:**

- `worker_oldest_uncommitted_offset_age_seconds` per service and partition (alert on high age). The timeout checker threshold is timeout + limit + margin (about 1h20m). The executor threshold sits above the transfer retry time (e.g. 5 min), so a normal retry does not fire it.
- `worker_permits_available` per service (UNORDERED and DELAYED modes).
- `accounts_held_total` (labels: `service`, `error_class`); alert on any increase.
- `unattributable_records_total` (labels: `service`, `topic`); alert on any increase.
- `transient_retries_total` (labels: `service`, `error_class`).
- `stuck_executing_redriven_total`; alert on any increase. This is the stuck-`EXECUTING` detector (spec change 19).
- `withdrawal_end_to_end_seconds_count{outcome="needs_review"}`; alert on any increase.

## 6. Layout

```
wallet/
  wallet-api/            REST API                                   -> image
  completion-service/    saga coordinator                           -> image
  transfer-executor/     calls TransferService                      -> image
  timeout-checker/       worker pool, DELAYED mode                           -> image
  bridge/                Streams to Kafka (Lambda handler; local runner on DynamoDB Local)
  mocks/                 verifiers, AML, TransferService, Beneficiary -> image(s)
  libs/
    ledger-core/         shared domain + DynamoDB money transactions
    worker-pool/         poll loop, KEYED queues / UNORDERED semaphore, offset tracker
    events/              Kafka message records + JSON mapping
  e2e-tests/             full flows on Testcontainers
```

Packages inside each app:

```
org.arso.wallet.<app>/
  domain/        only if the app has its own rules
  application/   use cases
  port/          interfaces owned by the core
  adapter/       rest, kafka, dynamodb, clients
```

Why the shared libs:

- `ledger-core`: API cancel, completion, executor and timeout checker all write the same money transactions (refund, complete, reject). One implementation keeps balance, `version` and `seq` rules correct. (spec 6)
- `worker-pool`: used by completion, executor and timeout checker. Hardest part to get right, so it is written and tested once. (spec 8.4)
- `events`: one definition of every Kafka message for the bridge, mocks and consumers. (spec 7)
- All are plain Java libraries, no Spring Boot app.

## 7. Phases

Each phase ends with passing tests.

### Phase 0 - Build setup
- Gradle multi-module build with a version catalog.
- Spring Boot 4, Java 25, MVC starter, `spring.threads.virtual.enabled=true`.
- Dependencies: AWS SDK v2 DynamoDB (sync, Apache HTTP client with explicit pool size), Spring Kafka, Resilience4j, Micrometer + Prometheus, Micrometer Tracing + OpenTelemetry, ULID, ArchUnit, Testcontainers (LocalStack, Kafka), WireMock.
- `docker-compose` for local LocalStack + Kafka.
- One Dockerfile per app.
- ArchUnit rules from spec 4, plus: no DynamoDB `Scan` in any module.
- Spec ref: 4.

### Phase 1 - `ledger-core` domain
- Money (long minor units + currency, default USD).
- Account (balance, status, version), LedgerEntry and event types, Payment with the state machine, verifications map, reject reasons, counterparty snapshot.
- Unit tests: every allowed and forbidden state change (including the account status rules in 3.4: hold, release, block / unblock refused on `ON_HOLD`), balance never negative, version and `seq` rules.
- `CrashPoint` hook: a small interface called at named steps (`after-step-write`, `after-executing`, `after-transfer`, `after-reject`). No-op in production; in tests it throws, so the record is not marked done and is redelivered like after a crash. Deterministic, no container kills. Lives here so it is not tied to the worker pool.
- Spec ref: 5, 5.9, 6.1, 12.

### Phase 2 - `ledger-core` DynamoDB
- Table creation (code for tests and local; IaC later). Streams with `NEW_IMAGE`. TTL on idempotency items.
- One `TransactWriteItems` builder per row of spec 6.2, plus the deposit dedup item, plus hold and release (`ACCOUNT#` status + version, `pre_hold_status`, `hold_refs`, `account_held` / `account_released` ledger entry). Hold condition: `attribute_not_exists(hold_refs) OR NOT contains(hold_refs, :ref)`. Release removes `pre_hold_status` and `hold_refs`.
- Hold and release tests: first hold sets `pre_hold_status` and `hold_refs`; a second hold adds its ref and keeps `pre_hold_status`; a repeat with the same `hold_ref` writes nothing; release removes both attributes.
- Optimistic lock retry: re-read, random backoff, default 3 tries, then fail (API: 503; consumers: transient error, see 5.6).
- Idempotency store (section 3.8): canonical hash, consistent lookup, success put in the transaction, error put, TTL-aware condition.
- Transaction outcome classifier and per-item condition handling (section 3.11). `ReturnValuesOnConditionCheckFailure=ALL_OLD` on every condition; `ClientRequestToken` on same-request retries.
- Tests: classifier unit tests with mixed reason lists, including several failed items (e.g. `ACCOUNT#` balance + `LEDGER#` + `IDEMPOTENCY#` resolves to replay); fake DynamoDB client returning chosen cancel reasons (LocalStack cannot reliably produce `TransactionConflict`): conflict on mark-executing ends `EXECUTING`, conflict on reject / complete / cancel ends in the right final state, timeout after commit + retry with the same token applies once.
- Ledger query: main table, newest first, paged by `seq` (see 3.2).
- Stale read tests: test DynamoDB client where an eventually consistent `GetItem` returns the previous version; unit check that every `PAYMENT#` `GetItem` sets `ConsistentRead=true`.
- LocalStack tests, including concurrent withdrawals on one account.
- Spec ref: 5, 6, 12.

### Phase 3 - `wallet-api`
- Endpoints from spec 10, including internal block / unblock, plus `POST /internal/accounts/{id}/release` (spec change 21). User operations on `ON_HOLD` give 409 `account_on_hold`; block / unblock on `ON_HOLD` give 409. Ledger endpoint uses `limit` + `before_seq` instead of `from` / `to` (spec change 5).
- `Idempotency-Key` required on all POSTs. Request flow from section 3.8: lookup first, replay or 409, then the operation; store final business errors.
- Tests: replay after balance drained / account blocked / beneficiary down returns the stored 202; same key with different body or path gives 409; two concurrent requests with one key give one operation and the same response; concurrent same-key withdraws that together drain the balance, race a block, or meet a beneficiary outage: the loser gets the winner's 202 and no error is stored; re-serialized body (other key order) matches; expired but not deleted item is treated as new; retry after 422 gets 422 even after a deposit.
- Tests: deposit retried with a new key and the same `external_tx_id`: same account returns the original deposit (one credit), other account gives 409, two at once give one credit (section 3.9). Block racing a withdraw: balance stays right, one wins. Cancel on a blocked account ends `CANCELLED` with a refund.
- Tests: deposit and withdraw on an `ON_HOLD` account give 409; cancel still works; release with `to` sets that status; release `to = ACTIVE` over a prior `BLOCKED` is refused without `confirm = true`; release on a non-held account gives 409; AML unblock cannot clear a hold; account `BLOCKED`, hold, second hold, then release `to = ACTIVE` without confirm is refused.
- Error mapping: 400, 404, 409, 422, 503. Failed requests logged separately, never as ledger entries.
- Beneficiary client behind a port. Withdraw fetches the beneficiary snapshot itself.
- `received_at` stored on `PAYMENT#`.
- Spec ref: 9.1, 9.2, 9.3 (API part), 9.5, 9.9, 10.

### Phase 4 - `events` + `bridge`
- `libs/events`: records for the 4 topic messages (section 5) + Jackson mapping. Round-trip JSON tests.
- Bridge plain class: only `INSERT` of `LEDGER#` items. Publish to `ledger-events`; `withdraw` also to `withdraw-requests`.
- Lambda handler (`DynamodbEvent`, Amazon `aws-lambda-java-events` + SDK v2 transformer). Packaging and the event source mapping later (section 8).
- Local runner polling the DynamoDB Local stream (LocalStack is not used: the free image has no Java 25 Lambda runtime).
- Acked idempotent producer with compression; send the whole stream batch async, flush, check all results, then advance the shard iterator (section 5.6).
- Any error fails the whole batch; nothing is skipped, no account hold (section 5.6, spec change 22).
- Tests: publish fails, nothing is skipped, record published after recovery; one record in the middle of a batch fails → iterator does not move, whole batch resent. A record that cannot be mapped → the shard stalls, nothing after it is published. Only `INSERT` of `LEDGER#` reaches Kafka. Round-trip tests include `account_held` and `account_released`.
- Spec ref: 7, 8.1.

### Phase 5 - `worker-pool`
- One poll thread. Three dispatch modes (spec change 18):
  - KEYED (completion): N workers, each with its own bounded queue, picked by `hash(payment_id) mod N`.
  - UNORDERED (executor): one virtual thread per record, capped by a `Semaphore(max-in-flight)`. The poll thread uses `tryAcquire()`; with no free permit it pauses all assigned partitions and resumes below a low mark.
  - DELAYED (timeout checker): a record is processed at its due time (`requested_at` + timeout). The poll thread pauses the partition until the head record is due and keeps calling `poll()`, so no rebalance. Due records go through the same permit pool as UNORDERED (`Semaphore(max-in-flight)`, default 64), so an outage or a catch-up after downtime cannot start an unbounded number of retrying threads. Before dispatch, every record is checked again for `now >= due`; a record not yet due is held back. A record may finish later than its due time (re-check timer); later records keep flowing, only the commit waits. Size the uncommitted cap for about 15 min of records, so a re-check does not pause the partition.
- Offset tracker per partition: commit only contiguous done offsets. Cap done-but-uncommitted offsets per partition; over the cap, pause that partition only, so one slow record cannot grow the tracker without limit.
- Backpressure: pause partitions when queues are full or no permit is free, resume when drained, keep calling `poll()`.
- Pause reasons: each partition keeps a set of reasons (`NOT_DUE`, `BACKPRESSURE`, `UNCOMMITTED_CAP`, `CIRCUIT_OPEN`). Pause when the first reason is added; resume only when the set is empty. Kafka has one pause flag per partition, so one feature must never resume a partition another feature paused.
- DELAYED re-check wait (up to 15 min) does not hold a permit: the permit is released before the wait and the timer takes a permit again when it fires.
- Replay command (small CLI in `worker-pool`): `replay --topic <t> --partition <p> --offset <o>` reads one record and publishes it again to the same topic. Used to recover a consumer hold from its `hold_ref`.
- Rebalance: stop dispatching revoked partitions, wait briefly, commit done offsets.
- Graceful shutdown: stop polling, finish workers, commit, exit.
- Consumer settings: small `max.poll.records`, cooperative-sticky assignor, static membership.
- Failure policy from section 5.6: error classifier by exception family (business / known transient / permanent / unknown), known transient retry without limit, unknown retry with a budget, account hold (via `ledger-core`) for permanent and over-budget unknown errors, metrics.
- Tests: a record that throws an unknown exception puts its account `ON_HOLD` after the budget; other payments are not blocked; the permit is freed. An SDK auth error (`ExpiredToken`) outlasting the budget is retried, no hold. A record with an unreadable key is counted and marked done.
- Shared acked producer setup (`acks=all`, idempotence, send-wait helper).
- Unit tests first, then Kafka tests: out-of-order completion, full queues without rebalance, add / remove instance, transient error keeps the offset uncommitted, poison record puts its account `ON_HOLD` and the partition keeps moving.
- UNORDERED tests: a slow record does not delay others in the same partition; no free permit pauses partitions without a rebalance; the uncommitted cap pauses only that partition; two records for the same payment run safely.
- DELAYED tests: a partition paused until its record is due causes no rebalance; a pending re-check holds the commit but not later records; DynamoDB down with many due records keeps in-flight at the permit cap; a backpressure resume does not release a record before it is due (no early refund). 1000 re-checks waiting and a due `PENDING` record behind them → rejected on time. Hold path with an old-enum reader, a missing `PAYMENT#` and a missing `ACCOUNT#` → held or unattributable, never stuck. Same `hold_ref` twice → one hold.
- Spec ref: 7, 8.4, 15.3.

### Phase 6 - `mocks`
- Verifiers: configured steps, random latency, publish to `verification-results`. Never write to DynamoDB.
- AML: consume `ledger-events`, block / unblock via internal API on a simple rule. On 409 `account_on_hold` it keeps the wanted block / unblock and applies it when it sees `account_released`. Test: block during a hold, release `to = ACTIVE` with confirm → AML blocks again.
- TransferService: modes ok / clear failure / timeout.
- Beneficiary: returns name, masked account, fingerprint, country.
- Spec ref: 8.2, 8.7, 2 (out of scope list).

### Phase 7 - `completion-service`
- Consume `verification-results` with the worker pool.
- Record the step with one `UpdateItem` returning the item, no separate read (section 3.10). A duplicate skips the write but is still evaluated. `late: true` when the payment is final.
- Any reject and `PENDING` → reject `VERIFICATION_REJECTED`.
- All required steps allowed and `PENDING` → mark `EXECUTING` (with `ACCOUNT#` ACTIVE check) → publish `transfer-requests`. Account blocked → reject `ACCOUNT_BLOCKED`.
- Already `EXECUTING` → publish again.
- Publish waits for the ack before the record is marked done (section 5.6).
- Required steps from `PAYMENT#.required_steps`, never from config.
- Store `details` cut to 100 characters (spec change 20). Test: 500-character `details` is stored cut and the payment completes normally.
- Tests (crash points): after `EXECUTING` commit, before publish → republished on redelivery; after the last step write, before `EXECUTING` → the duplicate step is evaluated and moves to `EXECUTING`; after reject, before offset commit → already final on redelivery, no second refund.
- Tests: publish fails or times out (test producer) → offset stays uncommitted, message sent after recovery.
- Tests: unknown-error budget used up → account `ON_HOLD`, payment stays `PENDING` and is rejected with a refund by the timeout checker; other payments of that account are rejected `ACCOUNT_BLOCKED` at mark executing.
- Spec ref: 6.2, 8.3, 9.3, 9.4, 9.8, 9.9.

### Phase 8 - `transfer-executor`
- Consume `transfer-requests` with the worker pool in UNORDERED mode (spec change 18). Consistent read of `PAYMENT#`; skip if not `EXECUTING` (section 3.10).
- Call TransferService with `payment_id` as idempotency key.
- Success → complete. Clear failure → reject `EXECUTION_FAILED`. Unknown → retry (exponential from 5s, jitter), then `NEEDS_REVIEW`. Never auto-refund.
- Circuit breaker: while open, pause partitions; waiting is not a retry attempt.
- `complete` failing after TransferService ok → transient error, retried; never `NEEDS_REVIEW` (section 5.6).
- Tests: `complete` throttled after TransferService ok → ends `COMPLETED`; slow TransferService call for one payment does not delay others; already-done records redelivered after a crash are skipped.
- Tests (crash points): after TransferService ok, before `complete` → called again with the same key on redelivery, ends `COMPLETED` once; after reject, before offset commit → no second refund. Same record twice in parallel → one `complete`, the second stops quietly, one ledger entry. Stale `PAYMENT#` read with the stale-read client → still processed.
- Tests: unknown-error budget used up → account `ON_HOLD`, payment stays `EXECUTING`, no refund, re-driven by the timeout checker.
- Spec ref: 8.5, 9.7.

### Phase 9 - `timeout-checker`
- Own consumer group on `withdraw-requests`, worker pool in DELAYED mode (failure policy from section 5.6: retry, account hold).
- At `requested_at` + timeout (default 1h): consistent read of `PAYMENT#` (section 3.10) and reject `VERIFICATION_TIMEOUT` if still `PENDING`.
- Safety net (spec change 14, table in 5.6): `EXECUTING` older than the limit (default 15 min) → republish `transfer-requests` (acked); newer → re-check at `executing_at` + limit.
- Tests: stuck `EXECUTING` past the limit is republished and ends final; `EXECUTING` entered 5 min before the check and still `EXECUTING` is re-driven at `executing_at` + 15 min; throttled reject is retried and ends `REJECTED`; poison record puts the account `ON_HOLD`; crash during a re-check wait → redelivered, checked again, no double action; crash after reject, before offset commit → no second refund; stale `PAYMENT#` read with the stale-read client → correct decision.
- Tests: unknown-error budget used up → account `ON_HOLD` with `hold_ref`, metric and alert fire; replay from `hold_ref` after release ends the payment correctly.
- Spec ref: 8.6, 9.6.

### Phase 10 - Observability
- Custom metrics from spec 15.4: DynamoDB latency via `ExecutionInterceptor`, worker pool gauges, business counters, end-to-end and stage histograms. Plus `accounts_held_total` and `unattributable_records_total` (section 5.6).
- No high-cardinality labels (`account_id`, `payment_id`, raw URLs).
- No metric reads DynamoDB by status; no `EXECUTING` or `NEEDS_REVIEW` count gauges (spec change 19).
- Tracing propagated through Kafka headers.
- Spec ref: 15.4.

### Phase 11 - End-to-end tests
- Every scenario from spec 13 on the full stack (Testcontainers), with short timeouts from config.
- Invariant checks from spec 12 after each scenario, plus: exactly one final state per payment, checked on `PAYMENT#` (not by counting events); `NEEDS_REVIEW` has no ledger entry and no refund; sum of ledger entries equals the account balance.
- Spec ref: 12, 13.

## 8. Later (not in this plan)

- EKS manifests, PDBs, KEDA, probes (spec 15).
- Lambda packaging for the bridge (spec 8.1), with the event source mapping filter from section 5.6.
- Load test at ~10k ops/sec (spec 1).
- DynamoDB capacity model: per-operation WCU / RCU table, account and table quota check (cross-review B3). The table stays on-demand with no capacity or quota set until then (spec 14). Note: at steady load, provisioned capacity is cheaper than on-demand above ~30% utilisation.
- Reconciliation job and `NEEDS_REVIEW` resolution (spec 14).
- Grafana alerting (spec 14, 15.4).
- Multi-region (spec 16).
- Live `EXECUTING` / `NEEDS_REVIEW` count gauges, if operations need them: sparse keys-only GSI (`OPEN#<status>#<shard>`, sort key `executing_at`, ~+2 WCU per withdrawal) or the reconciliation job.
- Stall guard for unknown errors: if many accounts are put on hold in a short window, retry unknown errors without limit instead and alert (turns a mass deploy bug into lag instead of many held accounts). Skipped for now: budget only.
