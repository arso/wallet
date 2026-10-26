package org.arso.wallet.events;

import java.time.Instant;
import java.util.Objects;

public record VerificationResult(
        String eventId, int schemaVersion, String paymentId, String step, boolean allowed, String details, Instant decidedAt) {

    public VerificationResult {
        Objects.requireNonNull(eventId, "event_id");
        Objects.requireNonNull(paymentId, "payment_id");
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(decidedAt, "decided_at");
    }
}
