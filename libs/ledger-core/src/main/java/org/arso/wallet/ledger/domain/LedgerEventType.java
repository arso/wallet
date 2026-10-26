package org.arso.wallet.ledger.domain;

public enum LedgerEventType {
    ACCOUNT_CREATED,
    DEPOSIT,
    WITHDRAW,
    WITHDRAW_COMPLETED,
    USER_CANCEL_WITHDRAW,
    REJECT_WITHDRAW,
    ACCOUNT_BLOCKED,
    ACCOUNT_UNBLOCKED,
    ACCOUNT_HELD,
    ACCOUNT_RELEASED;

    public String wire() {
        return name().toLowerCase();
    }

    public static LedgerEventType fromWire(String value) {
        return valueOf(value.toUpperCase());
    }
}
