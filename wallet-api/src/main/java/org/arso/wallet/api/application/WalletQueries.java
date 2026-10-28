package org.arso.wallet.api.application;

import org.arso.wallet.api.application.ResponseBodies.AccountBody;
import org.arso.wallet.api.application.ResponseBodies.LedgerPage;
import org.arso.wallet.api.application.ResponseBodies.PaymentBody;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.DomainException;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.port.LedgerStore;

import static org.arso.wallet.ledger.domain.DomainException.Code.INVALID_REQUEST;
import static org.arso.wallet.ledger.domain.DomainException.Code.NOT_FOUND;

public final class WalletQueries {

    private static final int MAX_PAGE = 200;

    private final LedgerStore store;

    public WalletQueries(LedgerStore store) {
        this.store = store;
    }

    public AccountBody account(AccountId id) {
        return ResponseBodies.account(store.findAccount(id).orElseThrow(() -> notFound("account", id)));
    }

    public PaymentBody payment(PaymentId id) {
        return ResponseBodies.payment(store.findPayment(id).orElseThrow(() -> notFound("payment", id)));
    }

    public LedgerPage ledger(AccountId id, Long beforeSeq, int limit) {
        if (limit < 1 || limit > MAX_PAGE || (beforeSeq != null && beforeSeq < 1)) {
            throw new DomainException(INVALID_REQUEST, "limit must be 1.." + MAX_PAGE + ", before_seq must be positive");
        }
        var entries = store.entries(id, beforeSeq, limit);
        if (entries.isEmpty() && beforeSeq == null) {
            throw notFound("account", id);
        }
        return ResponseBodies.ledger(entries, limit);
    }

    private static DomainException notFound(String what, Object id) {
        return new DomainException(NOT_FOUND, "unknown " + what + " " + id);
    }
}
