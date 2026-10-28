package org.arso.wallet.api;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.arso.wallet.testing.MetricsAssertions.assertNoIdsInLabels;
import static org.assertj.core.api.Assertions.assertThat;

class MetricsTest extends ApiTest {

    @Autowired
    MeterRegistry meters;

    @Test
    void recordsBusinessMetricsWithoutIdsInLabels() {
        var id = account(0);
        deposit(id, UUID.randomUUID().toString(), 100, "tx-m-" + id);
        deposit(id, UUID.randomUUID().toString(), 100, "tx-m-" + id);
        var paymentId = pendingWithdrawal(id, 30);
        withdraw(id, UUID.randomUUID().toString(), 1000);
        post("/payments/" + paymentId + "/cancel", null);
        post("/internal/accounts/" + id + "/block", "{\"reason\":\"aml\"}");

        assertThat(meters.find("api_errors_total").tags("route", "/accounts/{id}/withdrawals", "error", "insufficient_funds")
                .counter()).isNotNull();
        var endToEnd = meters.find("withdrawal_end_to_end").tags("outcome", "cancelled", "reason", "none").timer();
        var buckets = endToEnd.takeSnapshot().histogramCounts();
        assertThat(buckets[buckets.length - 1].bucket(TimeUnit.MINUTES)).isGreaterThan(60);
        assertThat(meters.find("http.server.requests").tag("uri", "/accounts/{id}/withdrawals").timer()).isNotNull();
        assertNoIdsInLabels(meters);
    }
}
