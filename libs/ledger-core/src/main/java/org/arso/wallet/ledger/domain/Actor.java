package org.arso.wallet.ledger.domain;

public enum Actor {
    USER,
    COMPLETION_SERVICE,
    TRANSFER_EXECUTOR,
    TIMEOUT_CHECKER,
    BRIDGE,
    AML,
    INTERNAL;

    public String wire() {
        return name().toLowerCase().replace('_', '-');
    }

    public static Actor fromWire(String value) {
        return valueOf(value.toUpperCase().replace('-', '_'));
    }
}
