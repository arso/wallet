package org.arso.wallet.executor.port;

import org.arso.wallet.ledger.domain.Payment;

public interface TransferService {

    sealed interface Result {

        record Sent(String externalTxId) implements Result {}

        record Rejected(String reason) implements Result {}

        record Unknown(String reason) implements Result {}
    }

    /** Sends the payment with its id as the idempotency key. {@code Unknown} means the money may have been sent. */
    Result transfer(Payment payment) throws InterruptedException;
}
