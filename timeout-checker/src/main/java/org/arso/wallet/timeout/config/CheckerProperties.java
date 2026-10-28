package org.arso.wallet.timeout.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.time.Duration;

@ConfigurationProperties("checker")
public record CheckerProperties(
        DynamoDb dynamodb, Lock lock, Kafka kafka, Workers workers, Duration verificationTimeout,
        Duration executingStuckAfter) {

    public record DynamoDb(String tableName, URI endpoint, String region, int maxConnections) {
    }

    public record Lock(int maxAttempts, Duration maxBackoff) {
    }

    public record Kafka(String bootstrapServers, String groupId, String instanceId) {
    }

    public record Workers(int maxInFlight, int unknownMaxAttempts, Duration unknownMaxElapsed) {
    }
}
