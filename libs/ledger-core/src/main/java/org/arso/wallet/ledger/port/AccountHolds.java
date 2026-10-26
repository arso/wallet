package org.arso.wallet.ledger.port;

import org.arso.wallet.ledger.domain.Actor;

public interface AccountHolds {

    enum Result {
        HELD,
        ALREADY_HELD,
        UNATTRIBUTABLE
    }

    Result holdAccount(String accountId, String reason, String holdRef, Actor actor);

    Result holdPaymentAccount(String paymentId, String reason, String holdRef, Actor actor);
}
