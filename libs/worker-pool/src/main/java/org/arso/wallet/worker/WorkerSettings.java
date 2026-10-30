package org.arso.wallet.worker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.arso.wallet.ledger.domain.Actor;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Function;

public record WorkerSettings(
        String service,
        Actor actor,
        String topic,
        Dispatch dispatch,
        HoldTarget holdTarget,
        int maxInFlight,
        int maxUncommitted,
        Duration pollTimeout,
        Duration revokeWait,
        Duration shutdownWait,
        Duration initialBackoff,
        Duration maxBackoff,
        int unknownMaxAttempts,
        Duration unknownMaxElapsed) {

    public sealed interface Dispatch {

        record Keyed(int workers, int queueCapacity) implements Dispatch {
        }

        record Unordered() implements Dispatch {
        }

        record Delayed(Function<ConsumerRecord<String, byte[]>, Instant> dueAt) implements Dispatch {

            public Delayed {
                Objects.requireNonNull(dueAt);
            }
        }
    }

    public enum HoldTarget {
        ACCOUNT_ID,
        PAYMENT_ID
    }

    public static WorkerSettings of(String service, Actor actor, String topic, Dispatch dispatch, HoldTarget holdTarget) {
        return new WorkerSettings(service, actor, topic, dispatch, holdTarget, 64, 10_000,
                Duration.ofMillis(100), Duration.ofSeconds(5), Duration.ofSeconds(30),
                Duration.ofMillis(100), Duration.ofSeconds(30), 10, Duration.ofMinutes(5));
    }

    public WorkerSettings withMaxInFlight(int maxInFlight) {
        return new WorkerSettings(service, actor, topic, dispatch, holdTarget, maxInFlight, maxUncommitted, pollTimeout,
                revokeWait, shutdownWait, initialBackoff, maxBackoff, unknownMaxAttempts, unknownMaxElapsed);
    }

    public WorkerSettings withMaxUncommitted(int maxUncommitted) {
        return new WorkerSettings(service, actor, topic, dispatch, holdTarget, maxInFlight, maxUncommitted, pollTimeout,
                revokeWait, shutdownWait, initialBackoff, maxBackoff, unknownMaxAttempts, unknownMaxElapsed);
    }

    public WorkerSettings withRetryBackoff(Duration initial, Duration max) {
        return new WorkerSettings(service, actor, topic, dispatch, holdTarget, maxInFlight, maxUncommitted, pollTimeout,
                revokeWait, shutdownWait, initial, max, unknownMaxAttempts, unknownMaxElapsed);
    }

    public WorkerSettings withUnknownErrorBudget(int maxAttempts, Duration maxElapsed) {
        return new WorkerSettings(service, actor, topic, dispatch, holdTarget, maxInFlight, maxUncommitted, pollTimeout,
                revokeWait, shutdownWait, initialBackoff, maxBackoff, maxAttempts, maxElapsed);
    }
}
