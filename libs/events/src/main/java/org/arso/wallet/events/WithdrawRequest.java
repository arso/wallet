package org.arso.wallet.events;

import java.time.Instant;
import java.util.Objects;

/**
 * A new withdrawal, sent to every verifier and to the timeout checker. Built from the {@code withdraw} ledger entry.
 *
 * @param schemaVersion lets consumers reject a payload shape they do not know
 * @param paymentId     the withdrawal (also the Kafka key)
 * @param accountId     account the money leaves
 * @param seq           the {@code withdraw} entry's position in the account's ledger, linking back to it
 * @param amount        amount requested, in minor units
 * @param currency      currency of {@code amount}
 * @param beneficiary   snapshot of who receives the money, for verifiers to check
 * @param requestedAt   when it was requested; the timeout checker rejects it if still pending after the timeout (1h)
 */
public record WithdrawRequest(
        int schemaVersion,
        String paymentId,
        String accountId,
        long seq,
        long amount,
        String currency,
        Beneficiary beneficiary,
        Instant requestedAt) {

    public WithdrawRequest {
        Objects.requireNonNull(paymentId, "payment_id");
        Objects.requireNonNull(accountId, "account_id");
        Objects.requireNonNull(currency, "currency");
        Objects.requireNonNull(beneficiary, "beneficiary");
        Objects.requireNonNull(requestedAt, "requested_at");
    }
}
