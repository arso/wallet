package org.arso.wallet.completion.config;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoAccountHolds;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoDbMetrics;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoDbClients;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoLedgerStore;
import org.arso.wallet.ledger.adapter.metrics.MicrometerLedgerMetrics;
import org.arso.wallet.ledger.application.PaymentTransitions;
import org.arso.wallet.ledger.application.Transactions;
import org.arso.wallet.ledger.port.AccountHolds;
import org.arso.wallet.ledger.port.LedgerMetrics;
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
    DynamoDbClient dynamoDbClient(CompletionProperties properties, MeterRegistry meters) {
        var config = properties.dynamodb();
        return DynamoDbClients.create(
                config.endpoint(), config.region(), config.maxConnections(), new DynamoDbMetrics(meters));
    }

    @Bean
    LedgerStore ledgerStore(DynamoDbClient client, CompletionProperties properties) {
        return new DynamoLedgerStore(client, properties.dynamodb().tableName(), Duration.ofHours(24));
    }

    @Bean
    LedgerMetrics ledgerMetrics(MeterRegistry meters) {
        return new MicrometerLedgerMetrics(meters);
    }

    @Bean
    PaymentTransitions paymentTransitions(
            LedgerStore store, CompletionProperties properties, Clock clock, LedgerMetrics metrics) {
        var lock = properties.lock();
        var retry = new Transactions.RetryPolicy(lock.maxAttempts(), lock.maxBackoff());
        return new PaymentTransitions(store, new Transactions(store, retry, metrics), clock, metrics);
    }

    @Bean
    AccountHolds accountHolds(DynamoDbClient client, CompletionProperties properties, Clock clock) {
        return new DynamoAccountHolds(client, properties.dynamodb().tableName(), clock);
    }
}
