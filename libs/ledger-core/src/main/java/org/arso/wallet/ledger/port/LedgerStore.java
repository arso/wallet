package org.arso.wallet.ledger.port;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.arso.wallet.ledger.domain.Account;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.LedgerEntry;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.domain.Verification;

public interface LedgerStore {

    Optional<Account> findAccount(AccountId id);

    Optional<Payment> findPayment(PaymentId id);

    Optional<LedgerEntry> findEntry(AccountId id, long seq);

    /** One write, no read: sets the step while the payment is PENDING or EXECUTING, else stores it late. */
    StepWrite recordVerification(PaymentId id, String step, Verification verification);

    List<LedgerEntry> entries(AccountId id, Long beforeSeq, int limit);

    Optional<StoredResponse> findResponse(String key, Instant now);

    boolean storeResponse(String key, StoredResponse response, Instant now);

    /**
     * Writes all items in one transaction.
     *
     * @throws ConditionFailedException when a condition fails, with the conflict that decides the outcome
     * @throws TransientStoreException on conflicts, throttling or timeouts; retry with the same token
     */
    void commit(List<TxItem> items, String token);
}
