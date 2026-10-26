package org.arso.wallet.ledger.domain;

public enum PaymentStatus {
    PENDING,
    EXECUTING,
    COMPLETED,
    REJECTED,
    CANCELLED,
    NEEDS_REVIEW;

    public boolean isFinal() {
        return this != PENDING && this != EXECUTING;
    }
}
