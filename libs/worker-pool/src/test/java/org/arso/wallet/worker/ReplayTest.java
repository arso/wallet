package org.arso.wallet.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.arso.wallet.testing.KafkaLocal;
import org.junit.jupiter.api.Test;

class ReplayTest extends PoolTest {

    @Test
    void replayPublishesTheRecordAgainWithItsHeaders() throws Exception {
        createTopic(1);
        var headers = new RecordHeaders().add("content-type", "application/json".getBytes());
        PRODUCER.send(new ProducerRecord<>(topic, 0, "p-1", "{\"a\":1}".getBytes(), headers)).get();
        send(0, "p-2", "{}");

        try (var producer = new AckedProducer(KafkaLocal.bootstrapServers(), Map.of())) {
            Replay.replay(KafkaLocal.bootstrapServers(), producer, new TopicPartition(topic, 0), 0, Duration.ofSeconds(10));
        }

        var records = readAll(3);
        assertThat(records).extracting(ConsumerRecord::key).containsExactly("p-1", "p-2", "p-1");
        assertThat(new String(records.get(2).value())).isEqualTo("{\"a\":1}");
        assertThat(records.get(2).headers().lastHeader("content-type").value()).isEqualTo("application/json".getBytes());
    }

    List<ConsumerRecord<String, byte[]>> readAll(int expected) {
        var config = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(topic));
            var records = new ArrayList<ConsumerRecord<String, byte[]>>();
            var deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (records.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            return records;
        }
    }
}
