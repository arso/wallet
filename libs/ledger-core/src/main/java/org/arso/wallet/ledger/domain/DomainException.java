package org.arso.wallet.ledger.domain;

public final class DomainException extends RuntimeException {

    public enum Code {
        INVALID_REQUEST,
        INVALID_AMOUNT,
        INVALID_CURRENCY,
        CURRENCY_MISMATCH,
        NOT_FOUND,
        INSUFFICIENT_FUNDS,
        ACCOUNT_BLOCKED,
        ACCOUNT_ON_HOLD,
        ACCOUNT_NOT_ON_HOLD,
        CONFIRM_REQUIRED,
        INVALID_TRANSITION,
        NOT_CANCELLABLE,
        DUPLICATE_DEPOSIT
    }

    private final Code code;

    public DomainException(Code code, String message) {
        super(message, null, false, false);
        this.code = code;
    }

    public Code code() {
        return code;
    }
}
