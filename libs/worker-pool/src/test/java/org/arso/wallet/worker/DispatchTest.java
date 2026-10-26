package org.arso.wallet.worker;

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.arso.wallet.worker.RecordHandler.Outcome;
import org.arso.wallet.worker.WorkerSettings.HoldTarget;
import org.arso.wallet.worker.WorkerSettings.Mode;
import org.junit.jupiter.api.Test;

class DispatchTest extends PoolTest {

    final Map<String, List<String>> byKey = new ConcurrentHashMap<>();
    final Map<Long, AtomicInteger> times = new ConcurrentHashMap<>();
    final CountDownLatch release = new CountDownLatch(1);

    @Test
    void keyedKeepsOrderPerKeyAndCommitsOnlyContiguousOffsets() throws Exception {
        createTopic(1);
        send(0, "a", "1");
        send(0, "b", "1");
        send(0, "a", "2");
        send(0, "b", "2");
        start(settings(Mode.KEYED, HoldTarget.PAYMENT_ID).withKeyed(4, 8), r -> {
            if (r.offset() == 0) {
                release.await();
            }
            byKey.computeIfAbsent(r.key(), _ -> new CopyOnWriteArrayList<>()).add(new String(r.value()));
            return Outcome.DONE;
        });

        await(() -> byKey.getOrDefault("b", List.of()).size() == 2);
        pause(Duration.ofMillis(500));
        assertThat(byKey).doesNotContainKey("a");
        assertThat(committed(0)).isZero();

        release.countDown();
        await(() -> committed(0) == 4);
        assertThat(byKey).containsEntry("a", List.of("1", "2")).containsEntry("b", List.of("1", "2"));
    }

    @Test
    void unorderedSlowRecordDoesNotDelayOthers() throws Exception {
        createTopic(1);
        for (int i = 0; i < 10; i++) {
            send(0, "k" + i, "v");
        }
        var done = new CopyOnWriteArrayList<Long>();
        start(settings(Mode.UNORDERED, HoldTarget.PAYMENT_ID), r -> {
            if (r.offset() == 0) {
                release.await();
            }
            done.add(r.offset());
            return Outcome.DONE;
        });

        await(() -> done.size() == 9);
        assertThat(committed(0)).isZero();
        release.countDown();
        await(() -> committed(0) == 10);
    }

    @Test
    void noFreePermitPausesWithoutRebalance() throws Exception {
        createTopic(1);
        for (int i = 0; i < 6; i++) {
            send(0, "k" + i, "v");
        }
        start(settings(Mode.UNORDERED, HoldTarget.PAYMENT_ID).withMaxInFlight(1), r -> {
            Thread.sleep(700);
            times.computeIfAbsent(r.offset(), _ -> new AtomicInteger()).incrementAndGet();
            return Outcome.DONE;
        }, Map.of(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 2000));
        await(() -> !times.isEmpty());
        var members = memberIds();

        await(() -> committed(0) == 6);

        assertThat(times.values()).allSatisfy(n -> assertThat(n).hasValue(1));
        assertThat(memberIds()).isEqualTo(members);
    }

    @Test
    void uncommittedCapPausesOnlyThatPartition() throws Exception {
        createTopic(2);
        for (int i = 0; i < 60; i++) {
            send(0, "a" + i, "v");
            send(1, "b" + i, "v");
        }
        var p0 = new AtomicInteger();
        start(settings(Mode.UNORDERED, HoldTarget.PAYMENT_ID).withMaxUncommitted(10), r -> {
            if (r.partition() == 0) {
                if (r.offset() == 0) {
                    release.await();
                }
                p0.incrementAndGet();
            }
            return Outcome.DONE;
        }, Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5));

        await(() -> committed(1) == 60);
        assertThat(p0.get()).isLessThan(30);
        assertThat(committed(0)).isZero();
        release.countDown();
        await(() -> committed(0) == 60);
    }

    @Test
    void secondInstanceTakesPartitionsAndEveryRecordIsHandled() throws Exception {
        createTopic(4);
        for (int i = 0; i < 200; i++) {
            send(i % 4, "k" + i, "v");
        }
        Set<String> seen = ConcurrentHashMap.newKeySet();
        RecordHandler handler = r -> {
            Thread.sleep(10);
            seen.add(r.key());
            return Outcome.DONE;
        };
        var settings = settings(Mode.UNORDERED, HoldTarget.PAYMENT_ID).withMaxInFlight(4);
        var first = start(settings, handler);
        await(() -> seen.size() >= 40);
        start(settings, handler);
        await(() -> seen.size() >= 120);
        first.stop();

        await(() -> committed().values().stream().mapToLong(Long::longValue).sum() == 200);
        assertThat(seen).hasSize(200);
    }

    @Test
    void delayedRecordIsHandledAtItsDueTimeWithoutRebalance() throws Exception {
        createTopic(1);
        var delay = Duration.ofSeconds(3);
        for (int i = 0; i < 3; i++) {
            send(0, "k" + i, "v");
        }
        var handledAt = new ConcurrentHashMap<Long, Instant>();
        var due = new ConcurrentHashMap<Long, Instant>();
        var settings = settings(Mode.DELAYED, HoldTarget.PAYMENT_ID).withDueAt(r -> {
            var at = Instant.ofEpochMilli(r.timestamp()).plus(delay);
            due.put(r.offset(), at);
            return at;
        });
        start(settings, r -> {
            handledAt.put(r.offset(), Instant.now());
            return Outcome.DONE;
        }, Map.of(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1500));
        await(() -> !due.isEmpty());
        var members = memberIds();

        await(() -> committed(0) == 3);

        assertThat(handledAt).hasSize(3);
        handledAt.forEach((offset, at) -> assertThat(at).isAfterOrEqualTo(due.get(offset)));
        assertThat(memberIds()).isEqualTo(members);
    }

    @Test
    void recheckHoldsTheCommitButNotLaterRecords() throws Exception {
        createTopic(1);
        for (int i = 0; i < 3; i++) {
            send(0, "k" + i, "v");
        }
        var done = new CopyOnWriteArrayList<Long>();
        var settings = settings(Mode.DELAYED, HoldTarget.PAYMENT_ID).withDueAt(r -> Instant.ofEpochMilli(r.timestamp()));
        start(settings, r -> {
            if (times.computeIfAbsent(r.offset(), _ -> new AtomicInteger()).incrementAndGet() == 1 && r.offset() == 0) {
                return new Outcome.RecheckAt(Instant.now().plusSeconds(2));
            }
            done.add(r.offset());
            return Outcome.DONE;
        });

        await(() -> done.containsAll(List.of(1L, 2L)));
        assertThat(committed(0)).isZero();
        await(() -> committed(0) == 3);
        assertThat(times.get(0L)).hasValue(2);
    }

    @Test
    void failingDueRecordsStayWithinThePermitCap() throws Exception {
        createTopic(1);
        for (int i = 0; i < 20; i++) {
            send(0, "k" + i, "v");
        }
        var running = new AtomicInteger();
        var maxRunning = new AtomicInteger();
        var outageEnds = Instant.now().plusSeconds(2);
        var settings = settings(Mode.DELAYED, HoldTarget.PAYMENT_ID)
                .withMaxInFlight(3)
                .withDueAt(r -> Instant.ofEpochMilli(r.timestamp()));
        start(settings, r -> {
            maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            try {
                Thread.sleep(20);
                if (Instant.now().isBefore(outageEnds)) {
                    throw ErrorClassifierTest.dynamo(HTTP_UNAVAILABLE, "ServiceUnavailable");
                }
                return Outcome.DONE;
            } finally {
                running.decrementAndGet();
            }
        });

        await(() -> committed(0) == 20);
        assertThat(maxRunning.get()).isLessThanOrEqualTo(3);
    }
}
