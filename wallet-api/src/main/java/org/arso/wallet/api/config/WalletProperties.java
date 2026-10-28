package org.arso.wallet.api.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.time.Duration;
import java.util.List;

@ConfigurationProperties("wallet")
public record WalletProperties(
        DynamoDb dynamodb,
        Lock lock,
        Idempotency idempotency,
        Verification verification,
        Beneficiary beneficiary,
        String defaultCurrency) {

    public record DynamoDb(String tableName, URI endpoint, String region, int maxConnections, boolean createTable) {
    }

    public record Lock(int maxAttempts, Duration maxBackoff) {
    }

    public record Idempotency(Duration ttl) {
    }

    public record Verification(List<String> steps) {
    }

    public record Beneficiary(URI url, Duration timeout) {
    }
}
