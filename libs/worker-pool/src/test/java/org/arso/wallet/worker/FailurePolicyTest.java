package org.arso.wallet.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.arso.wallet.events.VerificationResult;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.ledger.domain.AccountStatus;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.worker.RecordHandler.Outcome;
import org.arso.wallet.worker.WorkerSettings.HoldTarget;
import org.arso.wallet.worker.WorkerSettings.Mode;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;

class FailurePolicyTest extends PoolTest {

    final Set<Long> handled = ConcurrentHashMap.newKeySet();

    @Test
    void transientErrorKeepsTheOffsetUncommittedUntilItWorks() throws Exception {
        createTopic(1);
        for (int i = 0; i < 5; i++) {
            send(0, "k" + i, "v");
        }
        var failures = new AtomicInteger();
        start(settings(Mode.UNORDERED, HoldTarget.PAYMENT_ID).withBackoff(Duration.ofMillis(100), Duration.ofMillis(100)), r -> {
            if (r.offset() == 1 && failures.incrementAndGet() <= 10) {
                throw SdkClientException.create("dynamodb down");
            }
            handled.add(r.offset());
            return Outcome.DONE;
        });

        await(() -> handled.containsAll(Set.of(0L, 2L, 3L, 4L)));
        assertThat(committed(0)).isLessThanOrEqualTo(1);
        await(() -> committed(0) == 5);
        assertThat(counter("transient_retries_total")).isEqualTo(10);
        assertThat(counter("accounts_held_total")).isZero();
    }

    @Test
    void permanentErrorHoldsTheAccountAndThePartitionMovesOn() throws Exception {
        createTopic(1);
        var id = ledger.open();
        ledger.deposit(id, 100);
        var payment = ledger.withdraw(id, 40);
        send(0, payment.id().value(), "not json");
        send(0, payment.id().value(), "{\"event_id\":\"e\",\"schema_version\":1,\"payment_id\":\"p\",\"step\":\"aml\","
                + "\"allowed\":true,\"decided_at\":\"2026-10-26T12:00:00.000Z\"}");
        start(settings(Mode.KEYED, HoldTarget.PAYMENT_ID), r -> {
            EventJson.read(r.value(), VerificationResult.class);
            handled.add(r.offset());
            return Outcome.DONE;
        });

        await(() -> committed(0) == 2);
        assertThat(handled).containsExactly(1L);
        var account = ledger.account(id);
        assertThat(account.status()).isEqualTo(AccountStatus.ON_HOLD);
        assertThat(account.holdRefs()).containsExactly(topic + "/0/0");
        assertThat(counter("accounts_held_total")).isOne();
    }

    @Test
    void unknownErrorIsHeldAfterTheBudgetAndDoesNotBlockOthers() throws Exception {
        createTopic(1);
        var id = ledger.open();
        send(0, id.value(), "bug");
        for (int i = 0; i < 5; i++) {
            send(0, id.value(), "ok");
        }
        var attempts = new AtomicInteger();
        var pool = settings(Mode.UNORDERED, HoldTarget.ACCOUNT_ID)
                .withMaxInFlight(2)
                .withBackoff(Duration.ofMillis(200), Duration.ofMillis(200))
                .withUnknownBudget(3, Duration.ofMinutes(1));
        start(pool, r -> {
            if (r.offset() == 0) {
                attempts.incrementAndGet();
                throw new NullPointerException("mapping bug");
            }
            handled.add(r.offset());
            return Outcome.DONE;
        });

        await(() -> handled.size() == 5);
        await(() -> committed(0) == 6);
        assertThat(attempts).hasValue(3);
        assertThat(ledger.account(id).holdRefs()).containsExactly(topic + "/0/0");
        await(() -> meters.get("worker_permits_available").gauge().value() == 2);
    }

    @Test
    void authErrorOutlastingTheBudgetIsRetriedWithoutHold() throws Exception {
        createTopic(1);
        var id = ledger.open();
        send(0, id.value(), "v");
        var attempts = new AtomicInteger();
        start(settings(Mode.UNORDERED, HoldTarget.ACCOUNT_ID).withUnknownBudget(2, Duration.ofMinutes(1)), r -> {
            if (attempts.incrementAndGet() <= 6) {
                throw ErrorClassifierTest.dynamo(400, "ExpiredTokenException");
            }
            return Outcome.DONE;
        });

        await(() -> committed(0) == 1);
        assertThat(attempts).hasValue(7);
        assertThat(ledger.account(id).status()).isEqualTo(AccountStatus.ACTIVE);
        assertThat(counter("accounts_held_total")).isZero();
    }

    @Test
    void recordsWithoutAnAccountAreCountedAndMarkedDone() throws Exception {
        createTopic(1);
        send(0, null, "no key");
        send(0, "not-a-ulid", "bad key");
        send(0, PaymentId.random().value(), "unknown payment");
        start(settings(Mode.UNORDERED, HoldTarget.PAYMENT_ID), _ -> {
            throw new PermanentRecordException("schema_version 9");
        });

        await(() -> committed(0) == 3);
        assertThat(counter("unattributable_records_total")).isEqualTo(3);
    }
}
