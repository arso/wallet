package org.arso.wallet.ledger.application;

public final class LockRetriesExhaustedException extends RuntimeException {

    public LockRetriesExhaustedException(int attempts) {
        super("gave up after " + attempts + " attempts", null, false, false);
    }
}
