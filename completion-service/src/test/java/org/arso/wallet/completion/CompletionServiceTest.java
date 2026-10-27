package org.arso.wallet.completion;

import static org.arso.wallet.ledger.application.CrashPoint.AFTER_EXECUTING;
import static org.arso.wallet.ledger.application.CrashPoint.AFTER_REJECT;
import static org.arso.wallet.ledger.application.CrashPoint.AFTER_STEP_WRITE;
import static org.arso.wallet.testing.MetricsAssertions.assertNoIdsInLabels;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.arso.wallet.completion.adapter.kafka.KafkaTransferPublisher;
import org.arso.wallet.completion.port.TransferPublisher;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.VerificationResult;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.ledger.application.CrashPoint;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.AccountStatus;
import org.arso.wallet.ledger.domain.LedgerEntry.WithdrawRejected;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.domain.PaymentStatus;
import org.arso.wallet.ledger.domain.RejectReason;
import org.arso.wallet.ledger.domain.Ulid;
import io.micrometer.core.instrument.MeterRegistry;
import org.arso.wallet.testing.DynamoDbLocal;
import org.arso.wallet.testing.KafkaLocal;
import org.arso.wallet.testing.TestLedger;
import org.arso.wallet.worker.AckedProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(classes = CompletionServiceApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(CompletionServiceTest.Faults.class)
class CompletionServiceTest {

    static final TestLedger LEDGER = new TestLedger();
    static final String GROUP = "completion-" + UUID.randomUUID();
    static final KafkaProducer<String, byte[]> PRODUCER = new KafkaProducer<>(
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers()),
            new StringSerializer(),
            new ByteArraySerializer());
    static final Map<String, AtomicInteger> CRASHES = new ConcurrentHashMap<>();
    static final Map<PaymentId, Instant> PUBLISH_DOWN_UNTIL = new ConcurrentHashMap<>();
    static final List<PaymentId> PUBLISHED = new CopyOnWriteArrayList<>();

    @Autowired
    MeterRegistry meters;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("management.server.port", () -> "0");
        registry.add("completion.dynamodb.endpoint", () -> DynamoDbLocal.endpoint().toString());
        registry.add("completion.dynamodb.table-name", () -> LEDGER.table);
        registry.add("completion.lock.max-attempts", () -> "10");
        registry.add("completion.kafka.bootstrap-servers", KafkaLocal::bootstrapServers);
        registry.add("completion.kafka.group-id", () -> GROUP);
        registry.add("completion.workers.unknown-max-attempts", () -> "3");
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
        TransferPublisher recordingPublisher(AckedProducer producer) {
            var kafka = new KafkaTransferPublisher(producer);
            return payment -> {
                if (Instant.now().isBefore(PUBLISH_DOWN_UNTIL.getOrDefault(payment.id(), Instant.MIN))) {
                    throw new KafkaException("broker down");
                }
                kafka.publish(payment);
                PUBLISHED.add(payment.id());
            };
        }
    }

    @Test
    void allRequiredStepsAllowedMovesToExecutingAndPublishesTheTransfer() {
        var payment = pending(List.of("aml", "kyc"));

        result(payment, "aml", true, null);
        assertThat(waitFor(() -> LEDGER.payment(payment.id()).verifications().size() == 1)).isTrue();
        assertThat(LEDGER.payment(payment.id()).status()).isEqualTo(PaymentStatus.PENDING);
        result(payment, "kyc", true, null);

        awaitStatus(payment, PaymentStatus.EXECUTING);
        await(() -> published(payment) == 1);
        assertThat(LEDGER.payment(payment.id()).executingAt()).isNotNull();
    }

    @Test
    void rejectedStepRejectsAndRefunds() {
        var payment = pending(List.of("aml", "kyc"));

        result(payment, "kyc", false, "sanctions list match");

        awaitStatus(payment, PaymentStatus.REJECTED);
        var rejected = LEDGER.payment(payment.id());
        assertThat(rejected.rejectReason()).isEqualTo(RejectReason.VERIFICATION_REJECTED);
        assertThat(LEDGER.account(payment.accountId()).balance().amountMinor()).isEqualTo(100);
        assertThat(rejects(payment)).singleElement().satisfies(r -> assertThat(r.verifier()).isEqualTo("kyc"));
        assertThat(published(payment)).isZero();
    }

    @Test
    void duplicateResultOnAnExecutingPaymentIsPublishedAgain() {
        var payment = pending(List.of("aml"));
        result(payment, "aml", true, null);
        await(() -> published(payment) == 1);

        result(payment, "aml", true, null);

        await(() -> published(payment) == 2);
        assertThat(LEDGER.payment(payment.id()).status()).isEqualTo(PaymentStatus.EXECUTING);
    }

    @Test
    void resultAfterTheFinalStateIsStoredLate() {
        var payment = pending(List.of("aml"));
        LEDGER.cancel(payment);

        result(payment, "aml", true, null);

        await(() -> LEDGER.payment(payment.id()).verifications().containsKey("aml"));
        var cancelled = LEDGER.payment(payment.id());
        assertThat(cancelled.status()).isEqualTo(PaymentStatus.CANCELLED);
        assertThat(cancelled.verifications().get("aml").late()).isTrue();
        assertThat(published(payment)).isZero();
    }

    @Test
    void blockedAccountIsRejectedAtMarkExecuting() {
        var payment = pending(List.of("aml"));
        LEDGER.block(payment.accountId());

        result(payment, "aml", true, null);

        awaitStatus(payment, PaymentStatus.REJECTED);
        assertThat(LEDGER.payment(payment.id()).rejectReason()).isEqualTo(RejectReason.ACCOUNT_BLOCKED);
        assertThat(LEDGER.account(payment.accountId()).balance().amountMinor()).isEqualTo(100);
    }

    @Test
    void longDetailsAreCutAndThePaymentCompletesNormally() {
        var payment = pending(List.of("aml"));

        result(payment, "aml", true, "x".repeat(500));

        awaitStatus(payment, PaymentStatus.EXECUTING);
        assertThat(LEDGER.payment(payment.id()).verifications().get("aml").details()).hasSize(100);
    }

    @Test
    void crashAfterExecutingIsRepublishedOnRedelivery() {
        var payment = pending(List.of("aml"));
        crash(AFTER_EXECUTING, payment, 1);

        result(payment, "aml", true, null);

        await(() -> published(payment) == 1);
        assertThat(LEDGER.payment(payment.id()).status()).isEqualTo(PaymentStatus.EXECUTING);
    }

    @Test
    void crashAfterTheLastStepWriteMovesToExecutingOnRedelivery() {
        var payment = pending(List.of("aml"));
        crash(AFTER_STEP_WRITE, payment, 1);

        result(payment, "aml", true, null);

        await(() -> published(payment) == 1);
        assertThat(LEDGER.payment(payment.id()).status()).isEqualTo(PaymentStatus.EXECUTING);
    }

    @Test
    void crashAfterRejectRefundsOnce() {
        var payment = pending(List.of("aml"));
        crash(AFTER_REJECT, payment, 1);

        var offset = result(payment, "aml", false, null);

        awaitStatus(payment, PaymentStatus.REJECTED);
        await(() -> committed() > offset);
        assertThat(rejects(payment)).hasSize(1);
        assertThat(LEDGER.account(payment.accountId()).balance().amountMinor()).isEqualTo(100);
    }

    @Test
    void failedPublishKeepsTheOffsetUncommittedUntilItIsSent() throws Exception {
        var payment = pending(List.of("aml"));
        PUBLISH_DOWN_UNTIL.put(payment.id(), Instant.now().plusSeconds(2));

        var offset = result(payment, "aml", true, null);

        awaitStatus(payment, PaymentStatus.EXECUTING);
        assertThat(committed()).isLessThanOrEqualTo(offset);
        await(() -> published(payment) == 1);
        await(() -> committed() > offset);
    }

    @Test
    void unknownErrorBudgetHoldsTheAccountAndItsNextPaymentIsRejected() {
        var payment = pending(List.of("aml"));
        var next = LEDGER.withdraw(payment.accountId(), 10, List.of("aml"));
        crash(AFTER_STEP_WRITE, payment, 100);

        var offset = result(payment, "aml", true, null);

        await(() -> LEDGER.account(payment.accountId()).status() == AccountStatus.ON_HOLD);
        assertThat(LEDGER.account(payment.accountId()).holdRefs()).containsExactly(Events.VERIFICATION_RESULTS + "/0/" + offset);
        assertThat(LEDGER.payment(payment.id()).status()).isEqualTo(PaymentStatus.PENDING);

        result(next, "aml", true, null);

        awaitStatus(next, PaymentStatus.REJECTED);
        assertThat(LEDGER.payment(next.id()).rejectReason()).isEqualTo(RejectReason.ACCOUNT_BLOCKED);
    }

    @Test
    void traceContinuesFromTheResultToTheTransferRequest() {
        var payment = pending(List.of("aml"));
        var traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
        var headers = new RecordHeaders().add("traceparent", ("00-" + traceId + "-00f067aa0ba902b7-01").getBytes());

        result(payment, "aml", true, null, headers);

        await(() -> published(payment) == 1);
        var request = KafkaLocal.records(Events.TRANSFER_REQUESTS, payment.id().value()).getFirst();
        assertThat(new String(request.headers().lastHeader("traceparent").value())).startsWith("00-" + traceId + "-");
    }

    @Test
    void recordsMetricsWithoutIdsInLabels() {
        var payment = pending(List.of("aml", "kyc"));
        result(payment, "kyc", false, "no");
        awaitStatus(payment, PaymentStatus.REJECTED);

        result(payment, "aml", true, null);

        await(() -> meters.find("late_verification_results_total").tag("step", "aml").counter() != null);
        assertThat(meters.find("verification_step").tag("step", "kyc").timer()).isNotNull();
        assertThat(meters.find("withdrawal_end_to_end").tags("outcome", "rejected", "reason", "verification_rejected")
                .timer()).isNotNull();
        assertThat(meters.find("dynamodb_request").tags("operation", "UpdateItem", "outcome", "ok").timer()).isNotNull();
        assertThat(meters.find("worker_inflight").tag("service", "completion-service").gauge()).isNotNull();
        assertThat(meters.find("worker_paused_partitions").tag("reason", "backpressure").gauge()).isNotNull();
        assertNoIdsInLabels(meters);
    }

    static Payment pending(List<String> steps) {
        AccountId id = LEDGER.open();
        LEDGER.deposit(id, 100);
        return LEDGER.withdraw(id, 40, steps);
    }

    static long result(Payment payment, String step, boolean allowed, String details) {
        return result(payment, step, allowed, details, new RecordHeaders());
    }

    static long result(Payment payment, String step, boolean allowed, String details, Headers headers) {
        var result = new VerificationResult(Ulid.next(), 1, payment.id().value(), step, allowed, details, Instant.now());
        var value = EventJson.write(result);
        try {
            return PRODUCER.send(new ProducerRecord<>(Events.VERIFICATION_RESULTS, null, payment.id().value(), value, headers))
                    .get()
                    .offset();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    static void crash(String step, Payment payment, int times) {
        CRASHES.put(step + "/" + payment.id().value(), new AtomicInteger(times));
    }

    static long published(Payment payment) {
        return PUBLISHED.stream().filter(payment.id()::equals).count();
    }

    static List<WithdrawRejected> rejects(Payment payment) {
        return LEDGER.store.entries(payment.accountId(), null, 50).stream()
                .filter(WithdrawRejected.class::isInstance)
                .map(WithdrawRejected.class::cast)
                .toList();
    }

    static long committed() {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers()))) {
            var offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get();
            var committed = offsets.get(new TopicPartition(Events.VERIFICATION_RESULTS, 0));
            return committed == null ? 0 : committed.offset();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    static void awaitStatus(Payment payment, PaymentStatus status) {
        await(() -> LEDGER.payment(payment.id()).status() == status);
    }

    static void await(BooleanSupplier condition) {
        assertThat(waitFor(condition)).as("timed out").isTrue();
    }

    static boolean waitFor(BooleanSupplier condition) {
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }
}
