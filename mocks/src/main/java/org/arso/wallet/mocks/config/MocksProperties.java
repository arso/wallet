package org.arso.wallet.mocks.config;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mocks")
public record MocksProperties(String kafkaBootstrapServers, Verifiers verifiers, Aml aml, Transfer transfer) {

    public record Verifiers(
            boolean enabled, List<String> steps, Duration minLatency, Duration maxLatency, Duration slowLatency, int maxInFlight) {}

    public record Aml(boolean enabled, URI walletUrl, Duration timeout, long blockThreshold) {}

    public record Transfer(Duration delay) {}
}
