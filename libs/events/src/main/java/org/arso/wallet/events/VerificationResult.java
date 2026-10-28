package org.arso.wallet.events;

import java.time.Instant;
import java.util.Objects;

/**
 * One verifier's decision on one step of a withdrawal.
 *
 * @param eventId       new ULID per message, to trace it (duplicates on redelivery are fine)
 * @param schemaVersion lets consumers reject a payload shape they do not know
 * @param paymentId     the withdrawal decided on (also the Kafka key)
 * @param step          which verification step this is, e.g. {@code aml}
 * @param allowed       the decision; one {@code false} rejects the withdrawal
 * @param details       the verifier's explanation, mainly for a rejection
 * @param decidedAt     when the verifier decided, to measure step latency
 */
public record VerificationResult(
        String eventId,
        int schemaVersion,
        String paymentId,
        String step,
        boolean allowed,
        String details,
        Instant decidedAt
) {

    public VerificationResult {
        Objects.requireNonNull(eventId, "event_id");
        Objects.requireNonNull(paymentId, "payment_id");
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(decidedAt, "decided_at");
    }
}
