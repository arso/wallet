package org.arso.wallet.ledger.adapter.dynamodb;

import java.net.URI;
import java.util.List;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public final class DynamoDbClients {

    private DynamoDbClients() {}

    public static DynamoDbClient create(
            URI endpoint, String region, int maxConnections, ExecutionInterceptor... interceptors) {
        var builder = DynamoDbClient.builder()
                .region(Region.of(region))
                .overrideConfiguration(c -> c.executionInterceptors(List.of(interceptors)))
                .httpClientBuilder(Apache5HttpClient.builder().maxConnections(maxConnections));
        if (endpoint != null) {
            builder.endpointOverride(endpoint)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")));
        }
        return builder.build();
    }
}
