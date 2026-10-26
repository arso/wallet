package org.arso.wallet.mocks.port;

public interface WalletAdmin {

    enum Result {
        APPLIED,
        ON_HOLD,
        NOT_APPLICABLE
    }

    final class WalletUnavailableException extends RuntimeException {

        public WalletUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    Result block(String accountId, String reason, String idempotencyKey);

    Result unblock(String accountId, String reason, String idempotencyKey);
}
