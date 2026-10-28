package org.arso.wallet.completion.port;

import org.arso.wallet.ledger.domain.Payment;

public interface TransferPublisher {

    /**
     * Returns only after the broker acked (plan 5.6).
     */
    void publish(Payment payment) throws InterruptedException;
}
