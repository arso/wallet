package org.arso.wallet.worker;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.testing.KafkaLocal;
import org.arso.wallet.testing.TestLedger;
import org.arso.wallet.worker.WorkerSettings.HoldTarget;
import org.arso.wallet.worker.WorkerSettings.Mode;
import org.junit.jupiter.api.AfterEach;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

abstract class PoolTest {

    static final Admin ADMIN;
    static final KafkaProducer<String, byte[]> PRODUCER;

    static {
        ADMIN = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers()));
        PRODUCER = new KafkaProducer<>(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaLocal.bootstrapServers()),
                new StringSerializer(),
                new ByteArraySerializer());
    }

    final TestLedger ledger = new TestLedger();
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final String topic = "t-" + UUID.randomUUID();
    final String group = "g-" + UUID.randomUUID();
    private final List<WorkerPool> pools = new ArrayList<>();

    @AfterEach
    void stopPools() {
        pools.forEach(WorkerPool::stop);
    }

    WorkerSettings settings(Mode mode, HoldTarget target) {
        return WorkerSettings.of("test", Actor.COMPLETION_SERVICE, topic, mode, target)
                .withBackoff(Duration.ofMillis(10), Duration.ofMillis(50));
    }

    WorkerPool start(WorkerSettings settings, RecordHandler handler) {
        return start(settings, handler, Map.of());
    }

    WorkerPool start(WorkerSettings settings, RecordHandler handler, Map<String, Object> consumerOverrides) {
        var config = new HashMap<>(WorkerPool.consumerConfig(KafkaLocal.bootstrapServers(), group, null));
        config.putAll(consumerOverrides);
        var pool = new WorkerPool(config, settings, handler, ErrorClassifier.defaults(), ledger.holds, meters);
        pools.add(pool);
        pool.start();
        return pool;
    }

    void createTopic(int partitions) throws ExecutionException, InterruptedException {
        ADMIN.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
    }

    void send(int partition, String key, String value) throws ExecutionException, InterruptedException {
        PRODUCER.send(new ProducerRecord<>(topic, partition, key, value == null ? null : value.getBytes(StandardCharsets.UTF_8)))
                .get();
    }

    Map<Integer, Long> committed() {
        try {
            return ADMIN.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().partition(), e -> e.getValue().offset()));
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    long committed(int partition) {
        return committed().getOrDefault(partition, 0L);
    }

    Set<String> memberIds() throws ExecutionException, InterruptedException {
        return ADMIN.describeConsumerGroups(List.of(group)).all().get().get(group).members().stream()
                .map(MemberDescription::consumerId)
                .collect(Collectors.toSet());
    }

    double counter(String name) {
        return meters.find(name).counters().stream().mapToDouble(c -> c.count()).sum();
    }

    static void await(BooleanSupplier condition) {
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as("timed out").isLessThan(deadline);
            pause(Duration.ofMillis(20));
        }
    }

    static void pause(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
