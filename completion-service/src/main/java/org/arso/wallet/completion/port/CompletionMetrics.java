package org.arso.wallet.completion.port;

import java.time.Duration;

public interface CompletionMetrics {

    /**
     * Withdraw request to verifier decision, per step.
     */
    void stepDecided(String step, Duration elapsed);

    void lateResult(String step);
}
