package org.arso.wallet.ledger.adapter.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.port.LedgerMetrics;

import java.time.Duration;
import java.time.Instant;

public final class MicrometerLedgerMetrics implements LedgerMetrics {

    private final MeterRegistry meters;
    private final Counter lockRetries;
    private final Timer verification;

    public MicrometerLedgerMetrics(MeterRegistry meters) {
        this.meters = meters;
        this.lockRetries = meters.counter("optimistic_lock_retries_total");
        this.verification = timer("withdrawal_verification").register(meters);
    }

    @Override
    public void executing(Payment payment) {
        verification.record(between(payment.receivedAt(), payment.executingAt()));
    }

    @Override
    public void finalized(Payment payment) {
        var outcome = payment.status().name().toLowerCase();
        var reason = payment.rejectReason() == null ? "none" : payment.rejectReason().name().toLowerCase();
        timer("withdrawal_end_to_end")
                .tags("outcome", outcome, "reason", reason)
                .register(meters)
                .record(between(payment.receivedAt(), payment.finalizedAt()));
        if (payment.executingAt() != null) {
            timer("transfer")
                    .tags("outcome", outcome)
                    .register(meters)
                    .record(between(payment.executingAt(), payment.finalizedAt()));
        }
    }

    @Override
    public void lockRetry() {
        lockRetries.increment();
    }

    private static Timer.Builder timer(String name) {
        return Timer.builder(name).publishPercentileHistogram();
    }

    private static Duration between(Instant from, Instant to) {
        return from == null || to == null ? Duration.ZERO : Duration.between(from, to);
    }
}
