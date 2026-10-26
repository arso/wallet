package org.arso.wallet.api.config;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("wallet")
public record WalletProperties(
        DynamoDb dynamodb,
        Lock lock,
        Idempotency idempotency,
        Verification verification,
        Beneficiary beneficiary,
        String defaultCurrency) {

    public record DynamoDb(String tableName, URI endpoint, String region, int maxConnections, boolean createTable) {}

    public record Lock(int maxAttempts, Duration maxBackoff) {}

    public record Idempotency(Duration ttl) {}

    public record Verification(List<String> steps) {}

    public record Beneficiary(URI url, Duration timeout) {}
}
