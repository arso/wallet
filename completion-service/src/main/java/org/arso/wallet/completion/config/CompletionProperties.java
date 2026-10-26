package org.arso.wallet.completion.config;

import java.net.URI;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("completion")
public record CompletionProperties(DynamoDb dynamodb, Lock lock, Kafka kafka, Workers workers) {

    public record DynamoDb(String tableName, URI endpoint, String region, int maxConnections) {}

    public record Lock(int maxAttempts, Duration maxBackoff) {}

    public record Kafka(String bootstrapServers, String groupId, String instanceId) {}

    public record Workers(int count, int queueCapacity, int unknownMaxAttempts, Duration unknownMaxElapsed) {}
}
