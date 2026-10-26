package org.arso.wallet.ledger.port;

import org.arso.wallet.ledger.domain.Account;
import org.arso.wallet.ledger.domain.Payment;

public sealed interface Conflict {

    record ResponseExists(StoredResponse response) implements Conflict {}

    record DepositExists(DepositRecord deposit) implements Conflict {}

    record PaymentChanged(Payment current) implements Conflict {}

    /** Version or ledger seq taken by another writer, or the account is not active. {@code current} is null for a seq conflict. */
    record AccountChanged(Account current) implements Conflict {}
}
