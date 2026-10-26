package org.arso.wallet.bridge.adapter.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.arso.wallet.bridge.adapter.stream.StreamRunner;
import org.arso.wallet.bridge.application.Bridge;
import org.arso.wallet.bridge.application.EventMapper;
import org.arso.wallet.bridge.port.EventPublisher.PublishFailedException;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.LedgerEvent;
import org.arso.wallet.events.WithdrawRequest;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.ledger.domain.Counterparty;
import org.arso.wallet.testing.DynamoDbLocal;
import org.arso.wallet.testing.TestLedger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;

class KafkaEventPublisherTest {

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");

    static {
        KAFKA.start();
    }

    final TestLedger ledger = new TestLedger();
    final KafkaEventPublisher publisher = KafkaEventPublisher.create(KAFKA.getBootstrapServers(), Map.of());

    @AfterEach
    void close() {
        publisher.close();
    }

    @Test
    void streamIsPublishedToBothTopicsWithHeaders() throws InterruptedException {
        var id = ledger.open();
        ledger.deposit(id, 100);
        var payment = ledger.withdraw(id, 40);
        var withdraw = ledger.entry(id, 3);
        var runner = new StreamRunner(
                DynamoDbLocal.streamsClient(),
                StreamRunner.streamArn(ledger.client, ledger.table),
                new Bridge(publisher),
                new StreamRunner.Settings(ShardIteratorType.TRIM_HORIZON, Duration.ofMillis(20), Duration.ofMillis(20)));
        var thread = Thread.ofVirtual().start(runner);

        var events = consume(Events.LEDGER_EVENTS, id.value(), 3);
        var requests = consume(Events.WITHDRAW_REQUESTS, payment.id().value(), 1);
        runner.stop();
        thread.join();

        assertThat(events).extracting(r -> EventJson.read(r.value(), LedgerEvent.class).seq()).containsExactly(1L, 2L, 3L);
        assertThat(header(events.get(1), Events.EVENT_TYPE_HEADER)).isEqualTo("deposit");
        assertThat(header(events.get(1), Events.CONTENT_TYPE_HEADER)).isEqualTo(Events.JSON);
        var request = EventJson.read(requests.getFirst().value(), WithdrawRequest.class);
        assertThat(request.accountId()).isEqualTo(id.value());
        assertThat(request.amount()).isEqualTo(40);
        assertThat(request.requestedAt()).isEqualTo(withdraw.meta().recordedAt());
    }

    @Test
    void messageNotAckedFailsTheBatch() {
        try (var small = KafkaEventPublisher.create(
                KAFKA.getBootstrapServers(), Map.of(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 2048))) {
            var id = ledger.open();
            var huge = ledger.deposit(id, 100, new Counterparty("p-1", "x".repeat(5000), "****1234", "hmac:1", "PL"));
            var messages = List.of(EventMapper.messages(ledger.entry(id, 1)).getFirst(), EventMapper.messages(huge).getFirst());

            assertThatThrownBy(() -> small.publishAll(messages))
                    .isInstanceOf(PublishFailedException.class)
                    .hasCauseInstanceOf(RecordTooLargeException.class);
        }
    }

    static List<ConsumerRecord<String, byte[]>> consume(String topic, String key, int expected) {
        var settings = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<>(settings, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(topic));
            var found = new ArrayList<ConsumerRecord<String, byte[]>>();
            var deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (found.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                    if (key.equals(r.key())) {
                        found.add(r);
                    }
                });
            }
            return found;
        }
    }

    static String header(ConsumerRecord<String, byte[]> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
