# worker-pool

Kafka consumer library shared by the three services that move payments forward. Plan: phase 5, section 5.6.

## Why

A plain listener container (Spring Kafka) processes a partition in order, one record at a time, and handles errors per
container. The payment flow needs more than that:

- **No head-of-line blocking.** A transfer retry can take ~2.5 min. In partition order, every payment behind it would
  wait.
- **Records due later.** The timeout checker acts at `requested_at + 1h`. Sleeping inside a listener would exceed
  `max.poll.interval.ms` and trigger a rebalance.
- **No message is ever dropped or left stuck.** Offsets are committed only when contiguous. A record that can never be
  processed puts its account `ON_HOLD` instead of going to a DLQ (spec change 21).
- **Bounded resources.** In-flight work is capped. Back pressure pauses partitions but keeps calling `poll()`, so the
  consumer never leaves the group.

Writing this once and testing it hard is cheaper than getting it right three times.

### Why not Spring Kafka

Spring Kafka covers the consumer plumbing (manual async acks with contiguous commits, pause without rebalance, delayed
partitions), but not the parts that matter here:

- **A listener processes records one at a time per consumer thread.** Parallelism equals the number of consumers, not
  partitions. Handing records to an executor to get parallelism means writing our own dispatch anyway, and then
  `DefaultErrorHandler` no longer applies (it needs synchronous processing; it retries by seeking, which blocks the
  partition, and it recovers to a DLQ).
- **More partitions do not fix it.** They raise throughput, but a transfer retry (~2.5 min) or a timeout re-check (up to
  15 min) still blocks its partition. They also cost broker resources (replicas, smaller producer batches, worse
  compression), and the count can never go down.
- **One pause flag per partition.** We need a set of pause reasons, so a back pressure resume never undoes a `NOT_DUE`
  or `CIRCUIT_OPEN` pause.

Revisit if the pool becomes hard to maintain: a hybrid (Spring container for the consumer lifecycle, this dispatch and
failure policy in the listener) would remove the poll loop and offset tracking. Kafka share groups (KIP-932) would fit
the executor alone.

## Where

| Service                      | Topic                  | Mode        | Why this mode                                               |
|------------------------------|------------------------|-------------|-------------------------------------------------------------|
| completion-service (phase 7) | `verification-results` | `KEYED`     | results of one payment must be applied in order             |
| transfer-executor (phase 8)  | `transfer-requests`    | `UNORDERED` | one slow TransferService call must not delay other payments |
| timeout-checker (phase 9)    | `withdraw-requests`    | `DELAYED`   | each record is handled at its due time                      |

The verifier and AML mocks use it too (`UNORDERED`, `KEYED`), without holds. The bridge does not use it: it reads
DynamoDB Streams, not Kafka.

## How

An app gives a `RecordHandler` and `WorkerSettings`:

- The handler returns `DONE` for any business answer (including "already final"), or `RecheckAt(t)` to look again later
  (DELAYED). Exceptions go through the failure policy.
- Modes:
    - `KEYED`: one bounded queue per worker, picked by `hash(key)`. Order per key, parallel across keys.
    - `UNORDERED`: one virtual thread per record, capped by `maxInFlight`.
    - `DELAYED`: like `UNORDERED`, but a record starts only at `dueAt(record)`. A recheck releases its permit while
      waiting and holds only the commit.

Failure policy (`ErrorClassifier`, `FailurePolicy`):

| Error     | Examples                                                  | Result                                                                                     |
|-----------|-----------------------------------------------------------|--------------------------------------------------------------------------------------------|
| transient | AWS SDK and Kafka errors, lock retries exhausted          | retried with backoff, no limit; offset stays uncommitted                                   |
| permanent | bad JSON, `PermanentRecordException`, DynamoDB validation | account `ON_HOLD`, record done                                                             |
| unknown   | anything else (bugs)                                      | retried within a budget (default 10 attempts / 5 min), then account `ON_HOLD`, record done |

The hold finds the account from the record key (`HoldTarget`: account id or payment id) and records
`hold_ref = <topic>/<partition>/<offset>`. No account found: counted in `unattributable_records_total`, record done.

A partition is paused while any reason holds: `NOT_DUE`, `BACKPRESSURE`, `UNCOMMITTED_CAP`, `CIRCUIT_OPEN`
(`circuitOpen(true)`, for the executor's circuit breaker). It resumes only when all are gone.

## Recovering a hold

Fix the bug, release the account (`POST /internal/accounts/{id}/release`), then replay the record:

```
./gradlew :libs:worker-pool:run --args="--topic <t> --partition <p> --offset <o>"
```

## Metrics

`accounts_held_total`, `unattributable_records_total`, `transient_retries_total`, `worker_permits_available`,
`worker_oldest_uncommitted_offset_age_seconds`, `worker_inflight` (records started or queued to a worker),
`worker_queue_depth` (polled, not yet dispatched: waiting for their due time or for capacity),
`worker_paused_partitions{reason}` (`not_due`, `backpressure`, `uncommitted_cap`, `circuit_open`; a partition can count
under several), and `worker_record_seconds{topic}` (processing time per record, including retries). Alert on any
increase of the first two.

## Tracing

With an `ObservationRegistry`, each record is processed in an observation that continues the trace from its
`traceparent` header, and `AckedProducer` writes the current trace into the header of every record it sends.
