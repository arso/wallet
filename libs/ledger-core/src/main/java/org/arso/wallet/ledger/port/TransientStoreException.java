package org.arso.wallet.ledger.port;

/**
 * A store write failed for a temporary reason (transaction conflict, throttling, timeout, 5xx) and may even have been
 * applied. Retry the same items with the same token, so the store treats the retry as the same request. Keeps
 * DynamoDB exceptions out of the application layer.
 */
public final class TransientStoreException extends RuntimeException {

    public TransientStoreException(Throwable cause) {
        super(cause);
    }
}
