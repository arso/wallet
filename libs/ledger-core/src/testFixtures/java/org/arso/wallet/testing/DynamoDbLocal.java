package org.arso.wallet.testing;

import org.arso.wallet.ledger.adapter.dynamodb.TableSetup;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.net.URI;
import java.util.UUID;

public final class DynamoDbLocal {

    private static final GenericContainer<?> CONTAINER = new GenericContainer<>("amazon/dynamodb-local:3.3.2")
            .withExposedPorts(8000)
            .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");

    static {
        CONTAINER.start();
    }

    private DynamoDbLocal() {
    }

    public static URI endpoint() {
        return URI.create("http://" + CONTAINER.getHost() + ":" + CONTAINER.getMappedPort(8000));
    }

    public static DynamoDbClient client() {
        return DynamoDbClient.builder()
                .endpointOverride(endpoint())
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .build();
    }

    public static DynamoDbStreamsClient streamsClient() {
        return DynamoDbStreamsClient.builder()
                .endpointOverride(endpoint())
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .build();
    }

    public static String newTable(DynamoDbClient client) {
        var table = "wallet-test-" + UUID.randomUUID();
        TableSetup.createIfMissing(client, table);
        return table;
    }
}
