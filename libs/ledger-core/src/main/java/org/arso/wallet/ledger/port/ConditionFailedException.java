package org.arso.wallet.ledger.port;

public final class ConditionFailedException extends RuntimeException {

    private final Conflict conflict;

    public ConditionFailedException(Conflict conflict) {
        super(conflict.getClass().getSimpleName(), null, false, false);
        this.conflict = conflict;
    }

    public Conflict conflict() {
        return conflict;
    }
}
