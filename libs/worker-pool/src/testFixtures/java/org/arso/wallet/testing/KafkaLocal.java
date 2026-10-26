package org.arso.wallet.testing;

import org.testcontainers.kafka.KafkaContainer;

public final class KafkaLocal {

    private static final KafkaContainer CONTAINER = new KafkaContainer("apache/kafka:4.1.0");

    static {
        CONTAINER.start();
    }

    private KafkaLocal() {}

    public static String bootstrapServers() {
        return CONTAINER.getBootstrapServers();
    }
}
