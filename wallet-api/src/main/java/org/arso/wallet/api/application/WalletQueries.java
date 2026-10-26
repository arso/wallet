package org.arso.wallet.api.application;

import static org.arso.wallet.ledger.domain.DomainException.Code.INVALID_REQUEST;
import static org.arso.wallet.ledger.domain.DomainException.Code.NOT_FOUND;

import org.arso.wallet.api.application.Views.AccountView;
import org.arso.wallet.api.application.Views.LedgerPage;
import org.arso.wallet.api.application.Views.PaymentView;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.DomainException;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.port.LedgerStore;

public final class WalletQueries {

    private static final int MAX_PAGE = 200;

    private final LedgerStore store;

    public WalletQueries(LedgerStore store) {
        this.store = store;
    }

    public AccountView account(AccountId id) {
        return Views.account(store.findAccount(id).orElseThrow(() -> notFound("account", id)));
    }

    public PaymentView payment(PaymentId id) {
        return Views.payment(store.findPayment(id).orElseThrow(() -> notFound("payment", id)));
    }

    public LedgerPage ledger(AccountId id, Long beforeSeq, int limit) {
        if (limit < 1 || limit > MAX_PAGE || (beforeSeq != null && beforeSeq < 1)) {
            throw new DomainException(INVALID_REQUEST, "limit must be 1.." + MAX_PAGE + ", before_seq must be positive");
        }
        var entries = store.entries(id, beforeSeq, limit);
        if (entries.isEmpty() && beforeSeq == null) {
            throw notFound("account", id);
        }
        return Views.ledger(entries, limit);
    }

    private static DomainException notFound(String what, Object id) {
        return new DomainException(NOT_FOUND, "unknown " + what + " " + id);
    }
}
