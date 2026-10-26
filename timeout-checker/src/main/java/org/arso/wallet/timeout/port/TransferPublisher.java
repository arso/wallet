package org.arso.wallet.timeout.port;

import org.arso.wallet.ledger.domain.Payment;

public interface TransferPublisher {

    /** Re-drives a stuck EXECUTING payment. Returns only after the broker acked (plan 5.6). */
    void republish(Payment payment) throws InterruptedException;
}
