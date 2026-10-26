package org.arso.wallet.executor.config;

import java.net.URI;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("executor")
public record ExecutorProperties(DynamoDb dynamodb, Lock lock, Kafka kafka, Workers workers, Transfer transfer, Circuit circuit) {

    public record DynamoDb(String tableName, URI endpoint, String region, int maxConnections) {}

    public record Lock(int maxAttempts, Duration maxBackoff) {}

    public record Kafka(String bootstrapServers, String groupId, String instanceId) {}

    public record Workers(int maxInFlight, int unknownMaxAttempts, Duration unknownMaxElapsed) {}

    public record Transfer(URI url, Duration connectTimeout, Duration readTimeout, int retries, Duration initialBackoff) {}

    public record Circuit(int windowSize, int minimumCalls, float failureRate, Duration openWait, int halfOpenCalls) {}
}
