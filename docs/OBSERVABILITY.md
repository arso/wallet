# Observability

Six graphs that show whether the system is healthy and where errors come from, and how to follow one withdrawal in Jaeger ([below](#traces-in-jaeger)). All queries use the metrics listed below.

## Metrics

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
| worker pool: see [worker-pool](../libs/worker-pool/README.md#metrics) | | consumers |

## Graph the queries in Prometheus

1. Start everything: `make up-all`.
2. Open http://localhost:9090 and check Status > Target health: every app should be `up`. Prometheus scrapes `/actuator/prometheus` of each app on its management port ([docker/prometheus.yml](../docker/prometheus.yml)); apps that are not running show as down.
3. Send some traffic. Paste this into a terminal (needs `jq`); it runs for about a minute:

   ```bash
   B=http://localhost:8080
   post() { curl -s -o /dev/null -w '%{http_code} ' -X POST "$B$1" -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" -d "$2"; }
   for round in $(seq 30); do
     acc=$(curl -s -X POST $B/accounts -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" -d '{}' | jq -r .account_id)
     post /accounts/$acc/deposits '{"amount":1000,"currency":"USD","external_tx_id":"tx-'$(uuidgen)'","payer":{"payer_id":"p-1","name":"John","account_masked":"****1234","account_fingerprint":"hmac:1","country":"PL"}}'
     for b in b-1 b-2 b-3 b-reject-1 b-deny-aml b-timeout-1; do
       post /accounts/$acc/withdrawals '{"amount":10,"currency":"USD","beneficiary_id":"'$b'"}'
     done
     post /accounts/$acc/withdrawals '{"amount":100000,"currency":"USD","beneficiary_id":"b-1"}'
     post /payments/01M00000000000000000000000/cancel '{"reason":"test"}'
     echo; sleep 2
   done
   ```

   Each round creates an account, deposits 1000 and sends six withdrawals of 10: three that complete (`b-1`, `b-2`, `b-3`), one TransferService rejects (`b-reject-1`), one the AML check rejects (`b-deny-aml`) and one where the transfer times out (`b-timeout-1`). The timeouts are retried and end in `needs_review` only after about 5 minutes (`executor.workers.unknown-max-elapsed`). They also open the circuit breaker, which pauses every transfer-executor partition, so the other withdrawals wait too: graphs 5 and 6 show this, and completed withdrawals catch up once the breaker closes. Drop `b-timeout-1` from the list for traffic without this. It then makes two API errors: a withdrawal of 100000 (`insufficient_funds`) and a cancel of an unknown payment (`not_found`). Each line printed is one round's HTTP status codes.
4. Go to Query, paste one query below and click Execute.
5. Switch to the Graph tab. Set the time range (e.g. 15m) at the top left.
6. To show two queries in one view, click Add query and paste the second one.
7. Keep the page URL: it stores the queries and the time range, so you can reopen the same view later.

Queries with `rate(...[5m])` need a few minutes of traffic before lines show up. Error queries end with `or ... * 0` so they draw a flat 0 line when there are no errors, instead of showing an empty result.

## 1. API errors and latency

```promql
# Which share of requests per route fails with a server error (5xx)?
(
  sum by (uri) (rate(http_server_requests_seconds_count{service="wallet-api", status=~"5.."}[5m]))
  or sum by (uri) (rate(http_server_requests_seconds_count{service="wallet-api"}[5m])) * 0
)
/ sum by (uri) (rate(http_server_requests_seconds_count{service="wallet-api"}[5m]))
```

```promql
# How slow is the slowest 1% of requests per route?
histogram_quantile(0.99, sum by (le, uri) (rate(http_server_requests_seconds_bucket{service="wallet-api"}[5m])))
```

Share of 5xx answers per route (0 to 1), and p99 latency per route. A flat line at 0 is healthy (use the Table tab to see it); a gap means the route had no requests in the last 5 minutes. 4xx answers (e.g. `insufficient_funds`) are not counted here, see graph 2. To see it move, run `docker compose stop dynamodb` while sending traffic, then `docker compose start dynamodb` and `make restart-apps`: DynamoDB Local keeps data in memory, so the restart loses the table, and only a wallet-api start creates it again (the bridge also needs the new table's stream). `docker compose pause dynamodb` does not work for this: requests hang instead of failing. Bad: any 5xx, p99 going up.

## 2. Business rejections from the API

```promql
# Which business errors does the API return, and how often?
sum by (route, error) (rate(api_errors_total[5m]))
```

Expected client errors (`insufficient_funds`, `not_found`) next to worrying ones. Bad: `lock_retries_exhausted` (too much contention on one account), a sudden jump in `account_blocked`.

## 3. Withdrawal outcomes

```promql
# How do withdrawals end (completed, rejected and why, needs review)?
sum by (outcome, reason) (rate(withdrawal_end_to_end_seconds_count[5m]))
```

How withdrawals end. Bad: any `needs_review`, `rejected` with reason `execution_failed` (TransferService) or `verification_timeout` (a verifier), rejected growing while completed falls.

## 4. Withdrawal time and the slow stage

```promql
# How long does the slowest 1% of withdrawals take from request to final status?
histogram_quantile(0.99, sum by (le) (rate(withdrawal_end_to_end_seconds_bucket[5m])))
```

```promql
# Which verification step is slow?
histogram_quantile(0.99, sum by (le, step) (rate(verification_step_seconds_bucket[5m])))
```

```promql
# How long does the transfer take, from EXECUTING to final status?
histogram_quantile(0.99, sum by (le) (rate(transfer_seconds_bucket[5m])))
```

p99 from request to final status, then per verification step and for the transfer. Bad: end-to-end p99 going up; the other two show which stage causes it.

## 5. Kafka pipeline (worker pools)

```promql
# Which services keep retrying records, and because of which error?
sum by (service, error_class) (rate(transient_retries_total[5m]))
  or sum by (service) (rate(worker_record_seconds_count[5m])) * 0
```

```promql
# Which services gave up on records in the last hour (account held or no account found)?
sum by (service, error_class) (increase(accounts_held_total[1h]))
  or sum by (service) (increase(unattributable_records_total[1h]))
  or sum by (service) (rate(worker_record_seconds_count[5m])) * 0
```

```promql
# Which services have partitions paused for a reason other than waiting for a due time?
sum by (service, reason) (worker_paused_partitions{reason!="not_due"})
```

Retries per second by error class, records given up on in the last hour (account put on hold, or no account found), and partitions paused for a bad reason (`not_due` is normal for the timeout checker). Failed records never show as errors in `worker_record_seconds`, because the failure policy catches them and retries or holds. Retries with `CallNotPermittedException` are the open circuit breaker; `ResourceNotFoundException` means the DynamoDB table is missing. Bad: steady retries, any hold or unattributable record, `backpressure` staying above 0 (workers can't keep up), `uncommitted_cap` (one record holds back commits), `circuit_open` (TransferService down).

## 6. DynamoDB and TransferService

```promql
# Which services get throttled or failing DynamoDB calls?
sum by (service) (rate(dynamodb_request_seconds_count{outcome=~"throttled|error", operation!="CreateTable"}[5m]))
  or sum by (service) (rate(dynamodb_request_seconds_count[5m])) * 0
```

```promql
# How often does TransferService answer with something other than success?
sum by (status) (rate(http_client_requests_seconds_count{uri="/transfers", status!~"2.."}[5m]))
```

```promql
# Is the TransferService circuit breaker open (0 closed, 1 half-open, 2 open)?
max(circuit_breaker_state)
```

DynamoDB failures (`conditional_failed` and `transaction_conflict` are normal races, so they are left out; so is `CreateTable`, which fails on every local wallet-api start because the table already exists), TransferService answers other than 2xx (422 is a business reject, anything else an unknown outcome), circuit breaker (0 closed, 1 half-open, 2 open). Bad: any throttled or error call, non-422 errors, breaker above 0.

## Traces in Jaeger

Apps send spans over OTLP to Jaeger (`management.opentelemetry.tracing.export.otlp.endpoint`, default `http://localhost:4318/v1/traces`; sampling `management.tracing.sampling.probability`, 1.0 locally). The trace context travels between services in the Kafka `traceparent` header.

Open http://localhost:16686. A search always needs a Service; Tags narrow it down. Set Lookback to cover the time of the payment (default: last hour).

Each withdrawal has two traces:

- The Kafka path (verifiers, completion, executor, TransferService call): Service `transfer-executor`, Tags `messaging.kafka.message.key=<payment_id>`. Nothing found means the payment stopped earlier: try `completion-service`, then `mocks`.
- The REST call (with the beneficiary lookup): Service `wallet-api`, Tags `payment.id=<payment_id>`.

Only have an account id? List its payment ids, then search as above:

```bash
curl -s localhost:8080/accounts/<account_id>/ledger | jq -r '.entries[] | select(.payment_id) | "\(.recorded_at) \(.event_type) \(.payment_id)"'
```

Account traces: Service `mocks`, Tags `messaging.kafka.message.key=<account_id>` shows every ledger entry of the account; Service `wallet-api`, Tags `http.url=/accounts/<account_id>/deposits` (exact path) shows its REST calls.

Good to know:

- Jaeger warns about a missing root span. That is expected: the bridge starts the trace but sends no span itself.
- timeout-checker joins the Kafka trace an hour after the withdrawal, when its check is due.
- Use beneficiary `b-reject-1` or `b-deny-aml` to see where a rejected withdrawal stops.
- Jaeger keeps traces in memory: `make down-all` clears them.
