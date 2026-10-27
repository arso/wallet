package org.arso.wallet.executor;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.arso.wallet.ledger.application.CrashPoint.AFTER_REJECT;
import static org.arso.wallet.ledger.application.CrashPoint.AFTER_TRANSFER;
import static org.arso.wallet.testing.MetricsAssertions.assertNoIdsInLabels;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.TransferRequest;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.ledger.application.CrashPoint;
import org.arso.wallet.ledger.domain.AccountStatus;
import org.arso.wallet.ledger.domain.LedgerEntry;
import org.arso.wallet.ledger.domain.LedgerEntry.WithdrawCompleted;
import org.arso.wallet.ledger.domain.LedgerEntry.WithdrawRejected;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.domain.PaymentStatus;
import org.arso.wallet.ledger.domain.RejectReason;
import org.arso.wallet.ledger.domain.Ulid;
import org.arso.wallet.testing.DynamoDbLocal;
import org.arso.wallet.testing.KafkaLocal;
import org.arso.wallet.testing.TestLedger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@SpringBootTest(classes = TransferExecutorApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(TransferExecutorTest.Faults.class)
class TransferExecutorTest {

    static final TestLedger LEDGER = new TestLedger();
    static final String GROUP = "executor-" + UUID.randomUUID();
    static final WireMockServer TRANSFER_SERVICE = new WireMockServer(options().dynamicPort());
    static final KafkaProducer<String, byte[]> PRODUCER = new KafkaProducer<>(
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers()),
            new StringSerializer(),
            new ByteArraySerializer());
    static final Map<String, AtomicInteger> CRASHES = new ConcurrentHashMap<>();
    static final TestDynamoDb DYNAMO = new TestDynamoDb(LEDGER.client, LEDGER.table);

    static {
        TRANSFER_SERVICE.start();
        TRANSFER_SERVICE.stubFor(post(urlEqualTo("/transfers")).atPriority(10).willReturn(aResponse()
                .withStatus(HttpStatus.OK.value())
                .withHeader("Content-Type", "application/json")
                .withBody("{\"external_tx_id\":\"ts-{{request.headers.Idempotency-Key}}\"}")
                .withTransformers("response-template")));
    }

    @Autowired
    CircuitBreaker circuit;

    @Autowired
    MeterRegistry meters;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("management.server.port", () -> "0");
        registry.add("executor.dynamodb.endpoint", () -> DynamoDbLocal.endpoint().toString());
        registry.add("executor.dynamodb.table-name", () -> LEDGER.table);
        registry.add("executor.kafka.bootstrap-servers", KafkaLocal::bootstrapServers);
        registry.add("executor.kafka.group-id", () -> GROUP);
        registry.add("executor.workers.unknown-max-attempts", () -> "3");
        registry.add("executor.transfer.url", TRANSFER_SERVICE::baseUrl);
        registry.add("executor.transfer.read-timeout", () -> "500ms");
        registry.add("executor.transfer.retries", () -> "2");
        registry.add("executor.transfer.initial-backoff", () -> "100ms");
        registry.add("executor.circuit.window-size", () -> "5");
        registry.add("executor.circuit.minimum-calls", () -> "5");
        registry.add("executor.circuit.failure-rate", () -> "100");
        registry.add("executor.circuit.open-wait", () -> "1s");
        registry.add("executor.circuit.half-open-calls", () -> "1");
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
    void transferOkCompletesThePayment() {
        var payment = executing();

        send(payment);

        awaitStatus(payment, PaymentStatus.COMPLETED);
        assertThat(LEDGER.payment(payment.id()).externalTxId()).isEqualTo("ts-" + payment.id().value());
        assertThat(completions(payment)).hasSize(1);
        assertThat(balance(payment)).isEqualTo(60);
    }

    @Test
    void clearFailureRejectsAndRefunds() {
        var payment = executing();
        stub(payment, aResponse().withStatus(HttpStatus.UNPROCESSABLE_CONTENT.value()));

        send(payment);

        awaitStatus(payment, PaymentStatus.REJECTED);
        assertThat(LEDGER.payment(payment.id()).rejectReason()).isEqualTo(RejectReason.EXECUTION_FAILED);
        assertThat(balance(payment)).isEqualTo(100);
    }

    @Test
    void unknownOutcomeIsRetriedThenNeedsReviewWithoutRefundAndDoesNotDelayOthers() {
        var slow = executing();
        var fast = executing();
        stub(slow, aResponse().withStatus(HttpStatus.OK.value()).withFixedDelay(1500));

        send(slow);
        send(fast);

        awaitStatus(fast, PaymentStatus.COMPLETED);
        assertThat(LEDGER.payment(slow.id()).status()).isEqualTo(PaymentStatus.EXECUTING);
        awaitStatus(slow, PaymentStatus.NEEDS_REVIEW);
        assertThat(calls(slow)).isEqualTo(3);
        assertThat(balance(slow)).isEqualTo(60);
    }

    @Test
    void finishedPaymentRedeliveredIsSkipped() {
        var payment = executing();
        send(payment);
        awaitStatus(payment, PaymentStatus.COMPLETED);

        var offset = send(payment);

        await(() -> committed() > offset);
        assertThat(calls(payment)).isEqualTo(1);
    }

    @Test
    void throttledCompleteAfterTransferOkEndsCompleted() {
        var payment = executing();
        DYNAMO.throttle(payment.id().value(), 5);

        send(payment);

        awaitStatus(payment, PaymentStatus.COMPLETED);
        assertThat(completions(payment)).hasSize(1);
        assertThat(calls(payment)).isGreaterThan(1);
    }

    @Test
    void crashAfterTransferCallsAgainWithTheSameKeyAndCompletesOnce() {
        var payment = executing();
        crash(AFTER_TRANSFER, payment, 1);

        send(payment);

        awaitStatus(payment, PaymentStatus.COMPLETED);
        assertThat(calls(payment)).isEqualTo(2);
        assertThat(completions(payment)).hasSize(1);
        assertThat(balance(payment)).isEqualTo(60);
    }

    @Test
    void crashAfterRejectRefundsOnce() {
        var payment = executing();
        stub(payment, aResponse().withStatus(HttpStatus.UNPROCESSABLE_CONTENT.value()));
        crash(AFTER_REJECT, payment, 1);

        var offset = send(payment);

        awaitStatus(payment, PaymentStatus.REJECTED);
        await(() -> committed() > offset);
        assertThat(entries(payment, WithdrawRejected.class)).hasSize(1);
        assertThat(balance(payment)).isEqualTo(100);
    }

    @Test
    void sameRecordTwiceInParallelCompletesOnce() {
        var payment = executing();
        stub(payment, aResponse().withStatus(HttpStatus.OK.value()).withFixedDelay(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"external_tx_id\":\"ts-parallel\"}"));

        send(payment);
        var offset = send(payment);

        await(() -> committed() > offset);
        assertThat(calls(payment)).isEqualTo(2);
        assertThat(LEDGER.payment(payment.id()).status()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(completions(payment)).hasSize(1);
        assertThat(balance(payment)).isEqualTo(60);
    }

    @Test
    void staleReadIsNeverUsedForTheDecision() {
        var account = LEDGER.open();
        LEDGER.deposit(account, 100);
        var pending = LEDGER.withdraw(account, 40);
        DYNAMO.freezePayment(pending.id().value());
        var payment = LEDGER.executing(pending);

        send(payment);

        awaitStatus(payment, PaymentStatus.COMPLETED);
    }

    @Test
    void unknownErrorBudgetHoldsTheAccountWithoutRefund() {
        var payment = executing();
        crash(AFTER_TRANSFER, payment, 100);

        var offset = send(payment);

        await(() -> LEDGER.account(payment.accountId()).status() == AccountStatus.ON_HOLD);
        assertThat(LEDGER.account(payment.accountId()).holdRefs()).containsExactly(Events.TRANSFER_REQUESTS + "/0/" + offset);
        assertThat(LEDGER.payment(payment.id()).status()).isEqualTo(PaymentStatus.EXECUTING);
        assertThat(balance(payment)).isEqualTo(60);
    }

    @Test
    void openCircuitPausesPaymentsWithoutNeedsReviewAndRecovers() {
        var failing = List.of(executing(), executing(), executing());
        var outage = failing.stream().map(p -> stub(p, aResponse().withStatus(HttpStatus.SERVICE_UNAVAILABLE.value()))).toList();
        failing.forEach(TransferExecutorTest::send);

        await(() -> circuit.getState() == CircuitBreaker.State.OPEN);
        var waiting = executing();
        send(waiting);
        outage.forEach(TRANSFER_SERVICE::removeStub);
        failing.forEach(p -> assertThat(LEDGER.payment(p.id()).status()).isEqualTo(PaymentStatus.EXECUTING));

        for (var payment : failing) {
            awaitStatus(payment, PaymentStatus.COMPLETED);
        }
        awaitStatus(waiting, PaymentStatus.COMPLETED);
        assertThat(circuit.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void traceContinuesFromTheRequestToTheTransferServiceCall() {
        var payment = executing();
        var traceId = "4bf92f3577b34da6a3ce929d0e0e4736";

        send(payment, new RecordHeaders().add("traceparent", ("00-" + traceId + "-00f067aa0ba902b7-01").getBytes()));

        awaitStatus(payment, PaymentStatus.COMPLETED);
        var traced = TRANSFER_SERVICE.countRequestsMatching(postRequestedFor(urlEqualTo("/transfers"))
                .withHeader("Idempotency-Key", equalTo(payment.id().value()))
                .withHeader("traceparent", matching("00-" + traceId + "-.*"))
                .build());
        assertThat(traced.getCount()).isEqualTo(1);
    }

    @Test
    void recordsMetricsWithoutIdsInLabels() {
        var completed = executing();
        var rejected = executing();
        stub(rejected, aResponse().withStatus(HttpStatus.UNPROCESSABLE_CONTENT.value()));

        send(completed);
        send(rejected);

        awaitStatus(completed, PaymentStatus.COMPLETED);
        awaitStatus(rejected, PaymentStatus.REJECTED);
        assertThat(meters.find("http.client.requests").tags("uri", "/transfers", "status", "422").timer()).isNotNull();
        assertThat(meters.find("transfer").tag("outcome", "completed").timer()).isNotNull();
        assertThat(meters.find("withdrawal_end_to_end").tags("outcome", "rejected", "reason", "execution_failed")
                .timer()).isNotNull();
        assertThat(meters.find("circuit_breaker_state").gauge()).isNotNull();
        assertNoIdsInLabels(meters);
    }

    static Payment executing() {
        var account = LEDGER.open();
        LEDGER.deposit(account, 100);
        return LEDGER.executing(LEDGER.withdraw(account, 40));
    }

    static StubMapping stub(Payment payment, ResponseDefinitionBuilder response) {
        return TRANSFER_SERVICE.stubFor(post(urlEqualTo("/transfers"))
                .withHeader("Idempotency-Key", equalTo(payment.id().value()))
                .atPriority(1)
                .willReturn(response));
    }

    static long send(Payment payment) {
        return send(payment, new RecordHeaders());
    }

    static long send(Payment payment, Headers headers) {
        var request = new TransferRequest(
                Ulid.next(), Events.SCHEMA_VERSION, payment.id().value(), payment.accountId().value(),
                payment.amount().amountMinor(), payment.amount().currency().code(), payment.beneficiaryId());
        var value = EventJson.write(request);
        try {
            return PRODUCER.send(new ProducerRecord<>(Events.TRANSFER_REQUESTS, null, payment.id().value(), value, headers))
                    .get()
                    .offset();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    static void crash(String step, Payment payment, int times) {
        CRASHES.put(step + "/" + payment.id().value(), new AtomicInteger(times));
    }

    static int calls(Payment payment) {
        return TRANSFER_SERVICE.countRequestsMatching(postRequestedFor(urlEqualTo("/transfers"))
                        .withHeader("Idempotency-Key", equalTo(payment.id().value()))
                        .build())
                .getCount();
    }

    static long balance(Payment payment) {
        return LEDGER.account(payment.accountId()).balance().amountMinor();
    }

    static List<WithdrawCompleted> completions(Payment payment) {
        return entries(payment, WithdrawCompleted.class);
    }

    static <T extends LedgerEntry> List<T> entries(Payment payment, Class<T> type) {
        return LEDGER.store.entries(payment.accountId(), null, 50).stream()
                .filter(type::isInstance)
                .map(type::cast)
                .toList();
    }

    static long committed() {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers()))) {
            var offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get();
            var committed = offsets.get(new TopicPartition(Events.TRANSFER_REQUESTS, 0));
            return committed == null ? 0 : committed.offset();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
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
