package org.arso.wallet.worker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.arso.wallet.ledger.domain.Actor;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

public record WorkerSettings(
        String service,
        Actor actor,
        String topic,
        Mode mode,
        HoldTarget holdTarget,
        int keyedWorkers,
        int keyedQueueCapacity,
        int maxInFlight,
        int maxUncommitted,
        Function<ConsumerRecord<String, byte[]>, Instant> dueAt,
        Duration pollTimeout,
        Duration revokeWait,
        Duration shutdownWait,
        Duration initialBackoff,
        Duration maxBackoff,
        int unknownMaxAttempts,
        Duration unknownMaxElapsed) {

    public enum Mode {
        KEYED,
        UNORDERED,
        DELAYED
    }

    public enum HoldTarget {
        ACCOUNT_ID,
        PAYMENT_ID
    }

    public static WorkerSettings of(String service, Actor actor, String topic, Mode mode, HoldTarget holdTarget) {
        return new WorkerSettings(service, actor, topic, mode, holdTarget, 16, 64, 64, 10_000, null,
                Duration.ofMillis(100), Duration.ofSeconds(5), Duration.ofSeconds(30),
                Duration.ofMillis(100), Duration.ofSeconds(30), 10, Duration.ofMinutes(5));
    }

    public WorkerSettings withKeyed(int workers, int queueCapacity) {
        return new WorkerSettings(service, actor, topic, mode, holdTarget, workers, queueCapacity, maxInFlight,
                maxUncommitted, dueAt, pollTimeout, revokeWait, shutdownWait, initialBackoff, maxBackoff,
                unknownMaxAttempts, unknownMaxElapsed);
    }

    public WorkerSettings withMaxInFlight(int maxInFlight) {
        return new WorkerSettings(service, actor, topic, mode, holdTarget, keyedWorkers, keyedQueueCapacity, maxInFlight,
                maxUncommitted, dueAt, pollTimeout, revokeWait, shutdownWait, initialBackoff, maxBackoff,
                unknownMaxAttempts, unknownMaxElapsed);
    }

    public WorkerSettings withMaxUncommitted(int maxUncommitted) {
        return new WorkerSettings(service, actor, topic, mode, holdTarget, keyedWorkers, keyedQueueCapacity, maxInFlight,
                maxUncommitted, dueAt, pollTimeout, revokeWait, shutdownWait, initialBackoff, maxBackoff,
                unknownMaxAttempts, unknownMaxElapsed);
    }

    public WorkerSettings withDueAt(Function<ConsumerRecord<String, byte[]>, Instant> dueAt) {
        return new WorkerSettings(service, actor, topic, mode, holdTarget, keyedWorkers, keyedQueueCapacity, maxInFlight,
                maxUncommitted, dueAt, pollTimeout, revokeWait, shutdownWait, initialBackoff, maxBackoff,
                unknownMaxAttempts, unknownMaxElapsed);
    }

    public WorkerSettings withBackoff(Duration initial, Duration max) {
        return new WorkerSettings(service, actor, topic, mode, holdTarget, keyedWorkers, keyedQueueCapacity, maxInFlight,
                maxUncommitted, dueAt, pollTimeout, revokeWait, shutdownWait, initial, max, unknownMaxAttempts,
                unknownMaxElapsed);
    }

    public WorkerSettings withUnknownBudget(int maxAttempts, Duration maxElapsed) {
        return new WorkerSettings(service, actor, topic, mode, holdTarget, keyedWorkers, keyedQueueCapacity, maxInFlight,
                maxUncommitted, dueAt, pollTimeout, revokeWait, shutdownWait, initialBackoff, maxBackoff, maxAttempts,
                maxElapsed);
    }
}
