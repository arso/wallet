package org.arso.wallet.worker;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Sends a failed record again after its bug is fixed and its account is released. The {@code hold_ref} gives the
 * topic, partition and offset. Without this, a payment held by the timeout checker would never finish.
 */
public final class Replay {

    private Replay() {}

    public static void main(String[] args) throws InterruptedException {
        String topic = null;
        Integer partition = null;
        Long offset = null;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--topic" -> topic = args[i + 1];
                case "--partition" -> partition = Integer.parseInt(args[i + 1]);
                case "--offset" -> offset = Long.parseLong(args[i + 1]);
                default -> usage();
            }
        }
        if (topic == null || partition == null || offset == null) {
            usage();
            return;
        }
        var bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        try (var producer = new AckedProducer(bootstrap, Map.of())) {
            replay(bootstrap, producer, new TopicPartition(topic, partition), offset, Duration.ofSeconds(30));
            System.out.println("replayed " + topic + "/" + partition + "/" + offset);
        }
    }

    public static void replay(String bootstrap, AckedProducer producer, TopicPartition tp, long offset, Duration timeout)
            throws InterruptedException {
        var record = read(bootstrap, tp, offset, timeout);
        producer.sendAndWait(new ProducerRecord<>(tp.topic(), null, record.key(), record.value(), record.headers()));
    }

    private static ConsumerRecord<String, byte[]> read(String bootstrap, TopicPartition tp, long offset, Duration timeout) {
        var config = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.assign(List.of(tp));
            consumer.seek(tp, offset);
            var deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.offset() == offset) {
                        return record;
                    }
                    throw new IllegalArgumentException("no record at " + tp + "/" + offset + ", next is " + record.offset());
                }
            }
            throw new IllegalStateException("record " + tp + "/" + offset + " not read within " + timeout);
        }
    }

    private static void usage() {
        System.err.println("usage: replay --topic <t> --partition <p> --offset <o>");
        System.exit(2);
    }
}
