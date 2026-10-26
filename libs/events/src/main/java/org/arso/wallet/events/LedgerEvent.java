package org.arso.wallet.events;

import java.time.Instant;
import java.util.Objects;

public record LedgerEvent(
        int schemaVersion,
        String accountId,
        long seq,
        String eventType,
        Instant recordedAt,
        long balanceAfter,
        String actor,
        String reason,
        String paymentId,
        Long amount,
        String currency,
        String externalTxId,
        Payer payer,
        Beneficiary beneficiary,
        String verifier,
        String holdRef,
        String priorStatus,
        String releasedTo) {

    public LedgerEvent {
        Objects.requireNonNull(accountId, "account_id");
        Objects.requireNonNull(eventType, "event_type");
        Objects.requireNonNull(recordedAt, "recorded_at");
        Objects.requireNonNull(actor, "actor");
    }
}
