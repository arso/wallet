package org.arso.wallet.events;

import java.util.Objects;

/**
 * Asks the transfer executor to send a payment in {@code EXECUTING}. The executor reads the payment from DynamoDB, the
 * source of truth, so only {@code paymentId} drives it; the rest describes the transfer for tracing and logs.
 *
 * @param eventId       new ULID per message, to trace it (duplicates on redelivery are fine)
 * @param schemaVersion lets consumers reject a payload shape they do not know
 * @param paymentId     the payment to execute (also the Kafka key)
 * @param accountId     account the money leaves
 * @param amount        amount to send, in minor units
 * @param currency      currency of {@code amount}
 * @param beneficiaryId who receives the money
 */
public record TransferRequest(
        String eventId,
        int schemaVersion,
        String paymentId,
        String accountId,
        long amount,
        String currency,
        String beneficiaryId
) {

    public TransferRequest {
        Objects.requireNonNull(eventId, "event_id");
        Objects.requireNonNull(paymentId, "payment_id");
        Objects.requireNonNull(accountId, "account_id");
        Objects.requireNonNull(currency, "currency");
        Objects.requireNonNull(beneficiaryId, "beneficiary_id");
    }
}
