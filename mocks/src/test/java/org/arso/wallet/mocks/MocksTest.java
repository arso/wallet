package org.arso.wallet.mocks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.arso.wallet.events.Beneficiary;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.LedgerEvent;
import org.arso.wallet.events.VerificationResult;
import org.arso.wallet.events.WithdrawRequest;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.testing.KafkaLocal;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.test.web.servlet.client.RestTestClient.ResponseSpec;

@SpringBootTest(classes = MocksApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MocksTest {

    static final KafkaProducer<String, byte[]> PRODUCER = new KafkaProducer<>(
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers()),
            new StringSerializer(),
            new ByteArraySerializer());

    @LocalServerPort
    int port;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("management.server.port", () -> "0");
        registry.add("mocks.kafka-bootstrap-servers", KafkaLocal::bootstrapServers);
        registry.add("mocks.verifiers.steps", () -> "aml,kyc");
        registry.add("mocks.verifiers.min-latency", () -> "10ms");
        registry.add("mocks.verifiers.max-latency", () -> "50ms");
        registry.add("mocks.aml.wallet-url", () -> WalletStub.url().toString());
        registry.add("mocks.aml.block-threshold", () -> "5000");
        registry.add("mocks.verifiers.slow-latency", () -> "1s");
        registry.add("mocks.transfer.timeout-delay", () -> "2s");
    }

    @Test
    void beneficiaryIsStableAndMissingIdsAreNotFound() {
        var first = client(Duration.ofSeconds(5)).get().uri("/beneficiaries/b-1").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.beneficiary_id").isEqualTo("b-1")
                .jsonPath("$.account_masked").value(String.class, v -> assertThat(v).startsWith("****"))
                .jsonPath("$.country").isEqualTo("DE")
                .returnResult().getResponseBody();

        client(Duration.ofSeconds(5)).get().uri("/beneficiaries/b-1").exchange()
                .expectStatus().isOk()
                .expectBody(byte[].class).isEqualTo(first);
        client(Duration.ofSeconds(5)).get().uri("/beneficiaries/missing-1").exchange()
                .expectStatus().isNotFound();
    }

    @Test
    void transferIsIdempotentAndHasThreeModes() {
        var key = UUID.randomUUID().toString();
        var sent = transfer(key, "b-1", Duration.ofSeconds(5))
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.external_tx_id").value(String.class, v -> assertThat(v).startsWith("ts-"))
                .returnResult().getResponseBody();
        transfer(key, "b-1", Duration.ofSeconds(5))
                .expectStatus().isOk()
                .expectBody(byte[].class).isEqualTo(sent);

        transfer(UUID.randomUUID().toString(), "b-reject-1", Duration.ofSeconds(5))
                .expectStatus().isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT)
                .expectBody()
                .jsonPath("$.error").isEqualTo("transfer_rejected");

        assertThatThrownBy(() -> transfer(UUID.randomUUID().toString(), "b-timeout-1", Duration.ofMillis(300)))
                .hasRootCauseInstanceOf(HttpTimeoutException.class);
    }

    @Test
    void verifiersAnswerEachStepByRule() {
        var allowed = withdrawRequest("b-1");
        var denied = withdrawRequest("b-deny-kyc");
        var silent = withdrawRequest("b-silent-aml");
        var slow = withdrawRequest("b-slow-aml");

        var results = readResults(List.of(allowed.paymentId(), denied.paymentId(), silent.paymentId(), slow.paymentId()), 7);

        assertThat(results).filteredOn(r -> r.paymentId().equals(allowed.paymentId()))
                .extracting(r -> r.step() + "=" + r.allowed())
                .containsExactlyInAnyOrder("aml=true", "kyc=true");
        assertThat(results).filteredOn(r -> r.paymentId().equals(denied.paymentId()))
                .extracting(r -> r.step() + "=" + r.allowed())
                .containsExactlyInAnyOrder("aml=true", "kyc=false");
        assertThat(results).filteredOn(r -> r.paymentId().equals(silent.paymentId()))
                .extracting(r -> r.step() + "=" + r.allowed())
                .containsExactly("kyc=true");
        assertThat(results).filteredOn(r -> r.paymentId().equals(slow.paymentId()))
                .extracting(VerificationResult::step)
                .containsExactly("kyc", "aml");
    }

    @Test
    void amlBlocksOnLargeDepositAndUnblocksWhenCleared() {
        var account = AccountId.random().value();

        ledgerEvent(account, 2, "deposit", 9000L, "prov-1", null);
        ledgerEvent(account, 3, "deposit", 10L, "aml-unblock-1", null);

        await(() -> WalletStub.calls(account).size() == 2);
        assertThat(WalletStub.calls(account)).extracting(c -> c.path().substring(c.path().lastIndexOf('/') + 1))
                .containsExactly("block", "unblock");
        assertThat(WalletStub.calls(account).getFirst().idempotencyKey()).isEqualTo("aml-" + account + "-2");
    }

    @Test
    void blockRefusedDuringAHoldIsAppliedAfterRelease() {
        var account = AccountId.random().value();
        WalletStub.ON_HOLD.add(account);

        ledgerEvent(account, 2, "deposit", 9000L, "prov-1", null);
        await(() -> WalletStub.calls(account).size() == 1);
        WalletStub.ON_HOLD.remove(account);
        ledgerEvent(account, 3, Events.ACCOUNT_RELEASED, null, null, "ACTIVE");

        await(() -> WalletStub.calls(account).size() == 2);
        assertThat(WalletStub.calls(account).get(1).path()).endsWith("/block");
        assertThat(WalletStub.calls(account).get(1).idempotencyKey()).isEqualTo("aml-" + account + "-3");
    }

    WithdrawRequest withdrawRequest(String beneficiaryId) {
        var request = new WithdrawRequest(1, PaymentId.random().value(), AccountId.random().value(), 3, 2500, "USD",
                new Beneficiary(beneficiaryId, "Jane", "****1", "hmac:1", "DE"), Instant.now());
        send(Events.WITHDRAW_REQUESTS, request.paymentId(), request);
        return request;
    }

    void ledgerEvent(String account, long seq, String type, Long amount, String externalTxId, String releasedTo) {
        send(Events.LEDGER_EVENTS, account, new LedgerEvent(1, account, seq, type, Instant.now(), 0, "user", null, null,
                amount, amount == null ? null : "USD", externalTxId, null, null, null, null, null, releasedTo));
    }

    static void send(String topic, String key, Object message) {
        try {
            PRODUCER.send(new ProducerRecord<>(topic, key, EventJson.write(message))).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static List<VerificationResult> readResults(List<String> paymentIds, int expected) {
        var config = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(Events.VERIFICATION_RESULTS));
            var results = new ArrayList<VerificationResult>();
            var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (results.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                    if (paymentIds.contains(r.key())) {
                        results.add(EventJson.read(r.value(), VerificationResult.class));
                    }
                });
            }
            consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                if (paymentIds.contains(r.key())) {
                    results.add(EventJson.read(r.value(), VerificationResult.class));
                }
            });
            return results;
        }
    }

    ResponseSpec transfer(String key, String beneficiaryId, Duration timeout) {
        var body = "{\"payment_id\":\"%s\",\"account_id\":\"a\",\"amount\":100,\"currency\":\"USD\",\"beneficiary_id\":\"%s\"}"
                .formatted(key, beneficiaryId);
        return client(timeout).post().uri("/transfers")
                .header("Idempotency-Key", key)
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .exchange();
    }

    RestTestClient client(Duration timeout) {
        var factory = new JdkClientHttpRequestFactory();
        factory.setReadTimeout(timeout);
        return RestTestClient.bindToServer(factory).baseUrl("http://localhost:" + port).build();
    }

    static void await(BooleanSupplier condition) {
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as("timed out").isLessThan(deadline);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
