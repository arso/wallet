package org.arso.wallet.ledger.port;

import java.time.Instant;
import org.arso.wallet.ledger.domain.Account;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.LedgerEntry;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.domain.PaymentStatus;

public sealed interface TxItem {

    record PutAccount(Account account) implements TxItem {}

    record PutEntry(LedgerEntry entry) implements TxItem {}

    record PutPayment(Payment payment) implements TxItem {}

    record UpdatePayment(Payment payment, PaymentStatus expected, boolean requireAllowedSteps) implements TxItem {

        public UpdatePayment(Payment payment, PaymentStatus expected) {
            this(payment, expected, false);
        }
    }

    record CheckAccountActive(AccountId id) implements TxItem {}

    record PutResponse(String key, StoredResponse response, Instant now) implements TxItem {}

    record PutDeposit(DepositRecord deposit, Instant now) implements TxItem {}
}
