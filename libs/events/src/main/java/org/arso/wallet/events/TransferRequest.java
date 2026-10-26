package org.arso.wallet.events;

import java.util.Objects;

public record TransferRequest(
        String eventId, int schemaVersion, String paymentId, String accountId, long amount, String currency, String beneficiaryId) {

    public TransferRequest {
        Objects.requireNonNull(eventId, "event_id");
        Objects.requireNonNull(paymentId, "payment_id");
        Objects.requireNonNull(accountId, "account_id");
        Objects.requireNonNull(currency, "currency");
        Objects.requireNonNull(beneficiaryId, "beneficiary_id");
    }
}
