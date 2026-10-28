package org.arso.wallet.ledger.port;

import org.arso.wallet.ledger.domain.Actor;

/**
 * Puts an account {@code ON_HOLD} when a Kafka record about it cannot be processed. Used by the worker pool instead of a
 * dead letter queue: the failed record is marked done so its partition moves on, while the hold stops any further change
 * to the account until the bug is fixed, the account released and the record replayed.
 */
public interface AccountHolds {

    enum Result {
        /** This call put the account on hold; also when it was already held for a different record. */
        HELD,
        /** This same record ({@code hold_ref}) already holds the account, e.g. redelivered after a crash; nothing changed. */
        ALREADY_HELD,
        /** No account to hold: no key, an invalid id, or an unknown account or payment. The record is skipped. */
        UNATTRIBUTABLE
    }

    Result holdAccount(String accountId, String reason, String holdRef, Actor actor);

    Result holdPaymentAccount(String paymentId, String reason, String holdRef, Actor actor);
}
