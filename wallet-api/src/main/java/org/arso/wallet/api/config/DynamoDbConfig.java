package org.arso.wallet.api.config;

import org.arso.wallet.ledger.adapter.dynamodb.DynamoLedgerStore;
import org.arso.wallet.ledger.adapter.dynamodb.TableSetup;
import org.arso.wallet.ledger.application.Transactions;
import org.arso.wallet.ledger.port.LedgerStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Configuration(proxyBeanMethods = false)
public class DynamoDbConfig {

    @Bean
    DynamoDbClient dynamoDbClient(WalletProperties properties) {
        var config = properties.dynamodb();
        var builder = DynamoDbClient.builder()
                .region(Region.of(config.region()))
                .httpClientBuilder(Apache5HttpClient.builder().maxConnections(config.maxConnections()));
        if (config.endpoint() != null) {
            builder.endpointOverride(config.endpoint())
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")));
        }
        return builder.build();
    }

    @Bean
    LedgerStore ledgerStore(DynamoDbClient client, WalletProperties properties) {
        var table = properties.dynamodb().tableName();
        if (properties.dynamodb().createTable()) {
            TableSetup.createIfMissing(client, table);
        }
        return new DynamoLedgerStore(client, table, properties.idempotency().ttl());
    }

    @Bean
    Transactions transactions(LedgerStore store, WalletProperties properties) {
        return new Transactions(
                store, new Transactions.RetryPolicy(properties.lock().maxAttempts(), properties.lock().maxBackoff()));
    }
}
