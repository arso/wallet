package org.arso.wallet.worker;

/**
 * Thrown by a handler when the record itself is bad (e.g. unknown {@code schema_version}, invalid id), so a retry can
 * never succeed. The failure policy skips the retry budget and puts the account on hold right away.
 */
public final class PermanentRecordException extends RuntimeException {

    public PermanentRecordException(String message) {
        super(message);
    }
}
