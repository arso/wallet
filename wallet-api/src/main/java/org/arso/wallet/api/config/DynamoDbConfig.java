package org.arso.wallet.api.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoDbClients;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoDbMetrics;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoLedgerStore;
import org.arso.wallet.ledger.adapter.dynamodb.TableSetup;
import org.arso.wallet.ledger.adapter.metrics.MicrometerLedgerMetrics;
import org.arso.wallet.ledger.application.Transactions;
import org.arso.wallet.ledger.port.LedgerMetrics;
import org.arso.wallet.ledger.port.LedgerStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Configuration(proxyBeanMethods = false)
public class DynamoDbConfig {

    @Bean
    DynamoDbClient dynamoDbClient(WalletProperties properties, MeterRegistry meters) {
        var config = properties.dynamodb();
        return DynamoDbClients.create(
                config.endpoint(), config.region(), config.maxConnections(), new DynamoDbMetrics(meters));
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
    LedgerMetrics ledgerMetrics(MeterRegistry meters) {
        return new MicrometerLedgerMetrics(meters);
    }

    @Bean
    Transactions transactions(LedgerStore store, WalletProperties properties, LedgerMetrics metrics) {
        return new Transactions(
                store,
                new Transactions.RetryPolicy(properties.lock().maxAttempts(), properties.lock().maxBackoff()),
                metrics);
    }
}
