package org.arso.wallet.bridge.config;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import org.arso.wallet.bridge.adapter.kafka.KafkaEventPublisher;
import org.arso.wallet.bridge.adapter.stream.StreamRunner;
import org.arso.wallet.bridge.application.Bridge;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

public final class LocalRunnerMain {

    private LocalRunnerMain() {}

    public static void main(String[] args) {
        var endpoint = URI.create(Env.get("WALLET_DYNAMODB_ENDPOINT", "http://localhost:8000"));
        var region = Region.of(Env.get("WALLET_DYNAMODB_REGION", "us-east-1"));
        var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local"));
        var table = Env.get("WALLET_DYNAMODB_TABLE_NAME", "wallet-local");
        var publisher = KafkaEventPublisher.create(Env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), Map.of());
        String streamArn;
        try (var dynamo = DynamoDbClient.builder()
                .endpointOverride(endpoint)
                .region(region)
                .credentialsProvider(credentials)
                .httpClientBuilder(Apache5HttpClient.builder())
                .build()) {
            streamArn = StreamRunner.streamArn(dynamo, table);
        }
        var streams = DynamoDbStreamsClient.builder()
                .endpointOverride(endpoint)
                .region(region)
                .credentialsProvider(credentials)
                .httpClientBuilder(Apache5HttpClient.builder())
                .build();
        var runner = new StreamRunner(
                streams,
                streamArn,
                new Bridge(publisher),
                new StreamRunner.Settings(
                        ShardIteratorType.fromValue(Env.get("BRIDGE_START_AT", "TRIM_HORIZON")),
                        Duration.ofMillis(200),
                        Duration.ofSeconds(2)));
        var main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            runner.stop();
            try {
                main.join(Duration.ofSeconds(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            publisher.close();
        }));
        runner.run();
    }
}
