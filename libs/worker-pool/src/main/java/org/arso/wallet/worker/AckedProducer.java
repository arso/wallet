package org.arso.wallet.worker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class AckedProducer implements AutoCloseable {

    private final KafkaProducer<String, byte[]> producer;

    public AckedProducer(String bootstrapServers, Map<String, Object> overrides) {
        var settings = new HashMap<String, Object>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd",
                ProducerConfig.LINGER_MS_CONFIG, 5,
                ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024));
        settings.putAll(overrides);
        producer = new KafkaProducer<>(settings, new StringSerializer(), new ByteArraySerializer());
    }

    public void sendAndWait(ProducerRecord<String, byte[]> record) throws InterruptedException {
        try {
            producer.send(record).get();
        } catch (ExecutionException e) {
            throw e.getCause() instanceof KafkaException k ? k : new KafkaException(e.getCause());
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
