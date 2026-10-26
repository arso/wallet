package org.arso.wallet.events;

import java.time.Instant;
import java.util.Objects;

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
