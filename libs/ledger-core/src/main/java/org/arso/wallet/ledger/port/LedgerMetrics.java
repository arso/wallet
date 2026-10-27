package org.arso.wallet.ledger.port;

import org.arso.wallet.ledger.domain.Payment;

/** Business metrics of payment transitions (spec 15.4). Called only after the write committed. */
public interface LedgerMetrics {

    LedgerMetrics NONE = new LedgerMetrics() {};

    default void executing(Payment payment) {}

    default void finalized(Payment payment) {}

    default void lockRetry() {}
}
