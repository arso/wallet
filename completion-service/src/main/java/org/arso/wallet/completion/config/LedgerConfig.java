package org.arso.wallet.completion.config;

import java.time.Clock;
import java.time.Duration;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoAccountHolds;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoDbClients;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoLedgerStore;
import org.arso.wallet.ledger.application.PaymentTransitions;
import org.arso.wallet.ledger.application.Transactions;
import org.arso.wallet.ledger.port.AccountHolds;
import org.arso.wallet.ledger.port.LedgerStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Configuration(proxyBeanMethods = false)
public class LedgerConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    DynamoDbClient dynamoDbClient(CompletionProperties properties) {
        var config = properties.dynamodb();
        return DynamoDbClients.create(config.endpoint(), config.region(), config.maxConnections());
    }

    @Bean
    LedgerStore ledgerStore(DynamoDbClient client, CompletionProperties properties) {
        return new DynamoLedgerStore(client, properties.dynamodb().tableName(), Duration.ofHours(24));
    }

    @Bean
    PaymentTransitions paymentTransitions(LedgerStore store, CompletionProperties properties, Clock clock) {
        var lock = properties.lock();
        return new PaymentTransitions(
                store, new Transactions(store, new Transactions.RetryPolicy(lock.maxAttempts(), lock.maxBackoff())), clock);
    }

    @Bean
    AccountHolds accountHolds(DynamoDbClient client, CompletionProperties properties, Clock clock) {
        return new DynamoAccountHolds(client, properties.dynamodb().tableName(), clock);
    }
}
