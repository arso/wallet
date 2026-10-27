package org.arso.wallet.worker;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.SenderContext;
import java.nio.charset.StandardCharsets;
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
    private final ObservationRegistry observations;

    public AckedProducer(String bootstrapServers, Map<String, Object> overrides) {
        this(bootstrapServers, overrides, ObservationRegistry.NOOP);
    }

    /** Sends in an observation that writes the current trace into the {@code traceparent} header. */
    public AckedProducer(String bootstrapServers, Map<String, Object> overrides, ObservationRegistry observations) {
        this.observations = observations;
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
        var context = new SenderContext<ProducerRecord<String, byte[]>>(
                (carrier, key, value) -> carrier.headers().remove(key).add(key, value.getBytes(StandardCharsets.UTF_8)));
        context.setCarrier(record);
        Observation.createNotStarted("kafka.send", () -> context, observations)
                .contextualName(record.topic() + " send")
                .lowCardinalityKeyValue("topic", record.topic())
                .observeChecked(() -> {
                    try {
                        producer.send(record).get();
                    } catch (ExecutionException e) {
                        throw e.getCause() instanceof KafkaException k ? k : new KafkaException(e.getCause());
                    }
                });
    }

    @Override
    public void close() {
        producer.close();
    }
}
