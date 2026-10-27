package org.arso.wallet.completion.adapter.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import org.arso.wallet.completion.port.CompletionMetrics;

public final class MicrometerCompletionMetrics implements CompletionMetrics {

    private final MeterRegistry meters;

    public MicrometerCompletionMetrics(MeterRegistry meters) {
        this.meters = meters;
    }

    @Override
    public void stepDecided(String step, Duration elapsed) {
        Timer.builder("verification_step")
                .tag("step", step)
                .publishPercentileHistogram()
                .register(meters)
                .record(elapsed);
    }

    @Override
    public void lateResult(String step) {
        meters.counter("late_verification_results_total", "step", step).increment();
    }
}
