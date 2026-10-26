package org.arso.wallet.timeout;

import static org.arso.wallet.ledger.application.CrashPoint.AFTER_REJECT;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.arso.wallet.events.Beneficiary;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.WithdrawRequest;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.ledger.application.CrashPoint;
import org.arso.wallet.ledger.application.PaymentTransitions;
import org.arso.wallet.ledger.application.Transactions.Plan;
import org.arso.wallet.ledger.domain.AccountStatus;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.domain.LedgerEntry.WithdrawRejected;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.domain.PaymentStatus;
import org.arso.wallet.ledger.domain.RejectReason;
import org.arso.wallet.ledger.port.AccountHolds;
import org.arso.wallet.ledger.port.TxItem.PutAccount;
import org.arso.wallet.ledger.port.TxItem.PutEntry;
import org.arso.wallet.ledger.port.TxItem.UpdatePayment;
import org.arso.wallet.testing.DynamoDbLocal;
import org.arso.wallet.testing.KafkaLocal;
import org.arso.wallet.testing.TestLedger;
import org.arso.wallet.timeout.application.TimeoutChecker;
import org.arso.wallet.timeout.config.CheckerConfig;
import org.arso.wallet.timeout.config.CheckerProperties;
import org.arso.wallet.worker.AckedProducer;
import org.arso.wallet.worker.Replay;
import org.arso.wallet.worker.WorkerPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@SpringBootTest(classes = TimeoutCheckerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(TimeoutCheckerTest.Faults.class)
class TimeoutCheckerTest {

    static final Duration TIMEOUT = Duration.ofSeconds(2);
    static final Duration STUCK_AFTER = Duration.ofSeconds(3);
    static final TestLedger LEDGER = new TestLedger();
    static final PaymentTransitions TRANSITIONS = new PaymentTransitions(LEDGER.store, LEDGER.transactions, Clock.systemUTC());
    static final String GROUP = "checker-" + UUID.randomUUID();
    static final KafkaProducer<String, byte[]> PRODUCER = new KafkaProducer<>(
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers()),
            new StringSerializer(),
            new ByteArraySerializer());
    static final Map<String, AtomicInteger> CRASHES = new ConcurrentHashMap<>();
    static final TestDynamoDb DYNAMO = new TestDynamoDb(LEDGER.client, LEDGER.table);
    static WorkerPool restarted;

    @Autowired
    MeterRegistry meters;

    @Autowired
    WorkerPool pool;

    @Autowired
    TimeoutChecker checker;

    @Autowired
    AccountHolds holds;

    @Autowired
    CheckerProperties properties;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("management.server.port", () -> "0");
        registry.add("checker.dynamodb.endpoint", () -> DynamoDbLocal.endpoint().toString());
        registry.add("checker.dynamodb.table-name", () -> LEDGER.table);
        registry.add("checker.kafka.bootstrap-servers", KafkaLocal::bootstrapServers);
        registry.add("checker.kafka.group-id", () -> GROUP);
        registry.add("checker.workers.unknown-max-attempts", () -> "3");
        registry.add("checker.verification-timeout", TIMEOUT::toString);
        registry.add("checker.executing-stuck-after", STUCK_AFTER::toString);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Faults {

        @Bean
        @Primary
        CrashPoint testCrashPoint() {
            return (step, payment) -> {
                var left = CRASHES.get(step + "/" + payment.value());
                if (left != null && left.getAndDecrement() > 0) {
                    throw new IllegalStateException("simulated crash at " + step);
                }
            };
        }

        @Bean
        @Primary
        DynamoDbClient testDynamoDb() {
            return DYNAMO;
        }
    }

    @Test
    void pendingPastTheTimeoutIsRejectedAndRefunded() {
        var payment = pending();

        send(payment, past());

        awaitStatus(payment, PaymentStatus.REJECTED);
        assertThat(LEDGER.payment(payment.id()).rejectReason()).isEqualTo(RejectReason.VERIFICATION_TIMEOUT);
        assertThat(balance(payment)).isEqualTo(100);
    }

    @Test
    void pendingIsRejectedOnlyWhenDue() {
        var payment = pending();
        var requestedAt = now();

        send(payment, requestedAt);

        awaitStatus(payment, PaymentStatus.REJECTED);
        assertThat(LEDGER.payment(payment.id()).finalizedAt()).isAfterOrEqualTo(requestedAt.plus(TIMEOUT));
    }

    @Test
    void finalPaymentIsLeftAlone() {
        var payment = pending();
        LEDGER.cancel(payment);

        var offset = send(payment, past());

        await(() -> committed() > offset);
        assertThat(LEDGER.payment(payment.id()).status()).isEqualTo(PaymentStatus.CANCELLED);
        assertThat(transferRequests(payment)).isEmpty();
    }

    @Test
    void stuckExecutingPastTheLimitIsRepublishedAndEndsFinal() {
        var payment = executingSince(now().minus(Duration.ofHours(1)));
        var before = redriven();

        var offset = send(payment, past());

        await(() -> transferRequests(payment).size() == 1);
        await(() -> committed() > offset);
        assertThat(redriven()).isEqualTo(before + 1);
        TRANSITIONS.complete(payment.id(), "ts-1", Actor.TRANSFER_EXECUTOR);
        assertThat(LEDGER.payment(payment.id()).status()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(transferRequests(payment)).hasSize(1);
    }

    @Test
    void recentExecutingIsRedrivenAtExecutingAtPlusTheLimit() {
        var executingAt = now();
        var payment = executingSince(executingAt);

        var offset = send(payment, past());

        await(() -> transferRequests(payment).size() == 1);
        await(() -> committed() > offset);
        assertThat(Instant.ofEpochMilli(transferRequests(payment).getFirst().timestamp()))
                .isAfterOrEqualTo(executingAt.plus(STUCK_AFTER));
    }

    @Test
    void executingCompletedDuringTheWaitIsNotRepublished() {
        var payment = executingSince(now());

        var offset = send(payment, past());
        await(() -> DYNAMO.reads(payment.id().value()) > 0);
        TRANSITIONS.complete(payment.id(), "ts-1", Actor.TRANSFER_EXECUTOR);

        await(() -> committed() > offset);
        assertThat(DYNAMO.reads(payment.id().value())).isEqualTo(2);
        assertThat(transferRequests(payment)).isEmpty();
    }

    @Test
    void throttledRejectIsRetriedAndEndsRejected() {
        var payment = pending();
        DYNAMO.throttle(payment.id().value(), 5);

        send(payment, past());

        awaitStatus(payment, PaymentStatus.REJECTED);
        assertThat(entries(payment)).hasSize(1);
        assertThat(balance(payment)).isEqualTo(100);
    }

    @Test
    void poisonRecordHoldsTheAccount() {
        var payment = pending();

        var offset = send(payment.id().value(), "{not json".getBytes());

        await(() -> LEDGER.account(payment.accountId()).status() == AccountStatus.ON_HOLD);
        assertThat(LEDGER.account(payment.accountId()).holdRefs()).containsExactly(holdRef(offset));
        assertThat(LEDGER.payment(payment.id()).status()).isEqualTo(PaymentStatus.PENDING);
    }

    @Test
    void crashDuringTheRecheckWaitChecksAgainWithoutDoubleAction() {
        var payment = executingSince(now());
        var offset = send(payment, past());
        await(() -> DYNAMO.reads(payment.id().value()) == 1);

        pool.stop();
        restarted = new CheckerConfig().checkerPool(checker, holds, properties, meters);
        restarted.start();

        await(() -> DYNAMO.reads(payment.id().value()) >= 2);
        await(() -> committed() > offset);
        assertThat(transferRequests(payment)).hasSize(1);
    }

    @Test
    void crashAfterRejectRefundsOnce() {
        var payment = pending();
        crash(AFTER_REJECT, payment, 1);

        var offset = send(payment, past());

        awaitStatus(payment, PaymentStatus.REJECTED);
        await(() -> committed() > offset);
        assertThat(entries(payment)).hasSize(1);
        assertThat(balance(payment)).isEqualTo(100);
    }

    @Test
    void staleReadIsNeverUsedForTheDecision() {
        var payment = executingSince(now().minus(Duration.ofHours(1)));
        DYNAMO.freezePayment(payment.id().value());
        TRANSITIONS.complete(payment.id(), "ts-1", Actor.TRANSFER_EXECUTOR);

        var offset = send(payment, past());

        await(() -> committed() > offset);
        assertThat(transferRequests(payment)).isEmpty();
    }

    @Test
    void unknownErrorBudgetHoldsTheAccountAndReplayAfterReleaseRejects() throws InterruptedException {
        var payment = pending();
        var held = held();
        DYNAMO.breakPayment(payment.id().value(), 100);

        var offset = send(payment, past());

        await(() -> LEDGER.account(payment.accountId()).status() == AccountStatus.ON_HOLD);
        assertThat(LEDGER.account(payment.accountId()).holdRefs()).containsExactly(holdRef(offset));
        assertThat(LEDGER.payment(payment.id()).status()).isEqualTo(PaymentStatus.PENDING);
        assertThat(held()).isEqualTo(held + 1);

        DYNAMO.breakPayment(payment.id().value(), 0);
        release(payment);
        try (var producer = new AckedProducer(KafkaLocal.bootstrapServers(), Map.of())) {
            Replay.replay(KafkaLocal.bootstrapServers(), producer, partition(), offset, Duration.ofSeconds(10));
        }

        awaitStatus(payment, PaymentStatus.REJECTED);
        assertThat(LEDGER.payment(payment.id()).rejectReason()).isEqualTo(RejectReason.VERIFICATION_TIMEOUT);
        assertThat(balance(payment)).isEqualTo(100);
    }

    @AfterAll
    static void stopRestarted() {
        if (restarted != null) {
            restarted.stop();
        }
    }

    static Payment pending() {
        var account = LEDGER.open();
        LEDGER.deposit(account, 100);
        return LEDGER.withdraw(account, 40);
    }

    static Payment executingSince(Instant executingAt) {
        var p = LEDGER.executing(pending());
        var moved = new Payment(p.id(), p.accountId(), p.amount(), p.beneficiaryId(), p.status(), p.receivedAt(),
                executingAt, null, null, null, p.requiredSteps(), p.verifications());
        LEDGER.transactions.commit(List.of(new UpdatePayment(moved, PaymentStatus.EXECUTING)));
        return moved;
    }

    static void release(Payment payment) {
        LEDGER.transactions.onAccount(payment.accountId(), a -> {
            var change = a.release("fixed", AccountStatus.ACTIVE, false, Actor.INTERNAL, now());
            return new Plan<>(List.of(new PutAccount(change.account()), new PutEntry(change.entry())), change.entry());
        });
    }

    static long send(Payment payment, Instant requestedAt) {
        var request = new WithdrawRequest(
                Events.SCHEMA_VERSION, payment.id().value(), payment.accountId().value(), 2,
                payment.amount().amountMinor(), payment.amount().currency().code(),
                new Beneficiary(payment.beneficiaryId(), "Jane Doe", "****9876", "hmac:77aa", "DE"), requestedAt);
        return send(payment.id().value(), EventJson.write(request));
    }

    static long send(String key, byte[] value) {
        try {
            return PRODUCER.send(new ProducerRecord<>(Events.WITHDRAW_REQUESTS, key, value)).get().offset();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    static void crash(String step, Payment payment, int times) {
        CRASHES.put(step + "/" + payment.id().value(), new AtomicInteger(times));
    }

    static List<ConsumerRecord<String, byte[]>> transferRequests(Payment payment) {
        var config = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer())) {
            var tps = consumer.partitionsFor(Events.TRANSFER_REQUESTS).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();
            consumer.assign(tps);
            consumer.seekToBeginning(tps);
            var end = consumer.endOffsets(tps);
            var found = new ArrayList<ConsumerRecord<String, byte[]>>();
            while (tps.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                    if (payment.id().value().equals(r.key())) {
                        found.add(r);
                    }
                });
            }
            return found;
        }
    }

    static long committed() {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers()))) {
            var offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get();
            var committed = offsets.get(partition());
            return committed == null ? 0 : committed.offset();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    static TopicPartition partition() {
        return new TopicPartition(Events.WITHDRAW_REQUESTS, 0);
    }

    static String holdRef(long offset) {
        return Events.WITHDRAW_REQUESTS + "/0/" + offset;
    }

    double redriven() {
        var counter = meters.find("stuck_executing_redriven_total").counter();
        return counter == null ? 0 : counter.count();
    }

    double held() {
        return meters.find("accounts_held_total").tag("service", "timeout-checker").counters().stream()
                .mapToDouble(Counter::count)
                .sum();
    }

    static long balance(Payment payment) {
        return LEDGER.account(payment.accountId()).balance().amountMinor();
    }

    static List<WithdrawRejected> entries(Payment payment) {
        return LEDGER.store.entries(payment.accountId(), null, 50).stream()
                .filter(WithdrawRejected.class::isInstance)
                .map(WithdrawRejected.class::cast)
                .toList();
    }

    static Instant past() {
        return now().minus(Duration.ofMinutes(1));
    }

    static Instant now() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    static void awaitStatus(Payment payment, PaymentStatus status) {
        await(() -> LEDGER.payment(payment.id()).status() == status);
    }

    static void await(BooleanSupplier condition) {
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as("timed out").isLessThan(deadline);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }
}
