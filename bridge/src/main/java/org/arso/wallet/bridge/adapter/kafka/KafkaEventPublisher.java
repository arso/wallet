package org.arso.wallet.bridge.adapter.kafka;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.arso.wallet.bridge.port.EventPublisher;
import org.arso.wallet.bridge.port.EventPublisher.PublishFailedException;
import org.arso.wallet.events.json.EventJson;

public final class KafkaEventPublisher implements EventPublisher, AutoCloseable {

    private final Producer<String, byte[]> producer;

    public KafkaEventPublisher(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    public static KafkaEventPublisher create(String bootstrapServers, Map<String, Object> overrides) {
        var settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.ACKS_CONFIG, "all");
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        settings.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        settings.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        settings.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        settings.putAll(overrides);
        return new KafkaEventPublisher(new KafkaProducer<>(settings, new StringSerializer(), new ByteArraySerializer()));
    }

    @Override
    public void publishAll(List<Message> messages) {
        var sends = new ArrayList<Future<RecordMetadata>>(messages.size());
        for (var message : messages) {
            var value = EventJson.write(message.payload());
            sends.add(producer.send(new ProducerRecord<>(message.topic(), null, message.key(), value, headers(message))));
        }
        producer.flush();
        PublishFailedException failure = null;
        for (int i = 0; i < sends.size(); i++) {
            try {
                sends.get(i).get();
            } catch (ExecutionException e) {
                var error = new PublishFailedException(describe(messages.get(i)), e.getCause());
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PublishFailedException("interrupted", e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void close() {
        producer.close();
    }

    private static String describe(Message message) {
        return "not acked: " + message.topic() + " key=" + message.key();
    }

    private static RecordHeaders headers(Message message) {
        var headers = new RecordHeaders();
        message.headers().forEach((k, v) -> headers.add(k, v.getBytes(StandardCharsets.UTF_8)));
        return headers;
    }
}
