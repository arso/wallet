package org.arso.wallet.testing;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.time.Clock;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoAccountHolds;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoLedgerStore;
import org.arso.wallet.ledger.application.Transactions;
import org.arso.wallet.ledger.application.Transactions.Plan;
import org.arso.wallet.ledger.domain.Account;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.Counterparty;
import org.arso.wallet.ledger.domain.Currency;
import org.arso.wallet.ledger.domain.LedgerEntry;
import org.arso.wallet.ledger.domain.Money;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.port.TxItem.PutAccount;
import org.arso.wallet.ledger.port.TxItem.PutEntry;
import org.arso.wallet.ledger.port.TxItem.PutPayment;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public final class TestLedger {

    public static final Counterparty PARTY = new Counterparty("b-1", "Jane Doe", "****9876", "hmac:77aa", "DE");

    public final DynamoDbClient client = DynamoDbLocal.client();
    public final String table = DynamoDbLocal.newTable(client);
    public final DynamoLedgerStore store = new DynamoLedgerStore(client, table, Duration.ofHours(24));
    public final Transactions transactions = new Transactions(store, new Transactions.RetryPolicy(5, Duration.ofMillis(20)));
    public final DynamoAccountHolds holds = new DynamoAccountHolds(client, table, Clock.systemUTC());

    public AccountId open() {
        var id = AccountId.random();
        var change = Account.open(id, Currency.USD, now());
        transactions.commit(List.of(new PutAccount(change.account()), new PutEntry(change.entry())));
        return id;
    }

    public LedgerEntry deposit(AccountId id, long amount) {
        return deposit(id, amount, PARTY);
    }

    public LedgerEntry deposit(AccountId id, long amount, Counterparty payer) {
        return transactions.onAccount(id, a -> {
            var change = a.deposit(usd(amount), "prov-" + PaymentId.random(), payer, now());
            return new Plan<>(List.of(new PutAccount(change.account()), new PutEntry(change.entry())), change.entry());
        });
    }

    public Payment withdraw(AccountId id, long amount) {
        return transactions.onAccount(id, a -> {
            var paymentId = PaymentId.random();
            var change = a.withdraw(usd(amount), paymentId, PARTY, now());
            var payment = Payment.create(paymentId, id, usd(amount), PARTY.id(), List.of("aml"), now());
            return new Plan<>(
                    List.of(new PutAccount(change.account()), new PutEntry(change.entry()), new PutPayment(payment)),
                    payment);
        });
    }

    public Account account(AccountId id) {
        return store.findAccount(id).orElseThrow();
    }

    public LedgerEntry entry(AccountId id, long seq) {
        return store.findEntry(id, seq).orElseThrow();
    }

    static Money usd(long amount) {
        return new Money(amount, Currency.USD);
    }

    static Instant now() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }
}
