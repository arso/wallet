package org.arso.wallet.testing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class KafkaLocal {

    private static final KafkaContainer CONTAINER = new KafkaContainer("apache/kafka:4.1.0");

    static {
        CONTAINER.start();
    }

    private KafkaLocal() {
    }

    public static String bootstrapServers() {
        return CONTAINER.getBootstrapServers();
    }

    /**
     * All records with this key currently in the topic, read without a consumer group.
     */
    public static List<ConsumerRecord<String, byte[]>> records(String topic, String key) {
        var config = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer())) {
            var tps = consumer.partitionsFor(topic).stream().map(p -> new TopicPartition(p.topic(), p.partition())).toList();
            consumer.assign(tps);
            consumer.seekToBeginning(tps);
            var end = consumer.endOffsets(tps);
            var found = new ArrayList<ConsumerRecord<String, byte[]>>();
            while (tps.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                    if (key.equals(r.key())) {
                        found.add(r);
                    }
                });
            }
            return found;
        }
    }
}
