package org.arso.wallet.testing;

import org.arso.wallet.ledger.adapter.dynamodb.DynamoAccountHolds;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoLedgerStore;
import org.arso.wallet.ledger.application.PaymentTransitions;
import org.arso.wallet.ledger.application.Transactions;
import org.arso.wallet.ledger.application.Transactions.Writes;
import org.arso.wallet.ledger.domain.*;
import org.arso.wallet.ledger.port.StepWrite;
import org.arso.wallet.ledger.port.TxItem.PutAccount;
import org.arso.wallet.ledger.port.TxItem.PutEntry;
import org.arso.wallet.ledger.port.TxItem.PutPayment;
import org.arso.wallet.ledger.port.TxItem.UpdatePayment;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public final class TestLedger {

    public static final Counterparty PARTY = new Counterparty("b-1", "Jane Doe", "****9876", "hmac:77aa", "DE");

    public final DynamoDbClient client;
    public final String table;
    public final DynamoLedgerStore store;
    public final Transactions transactions;
    public final DynamoAccountHolds holds;

    public TestLedger() {
        this(DynamoDbLocal.newTable(DynamoDbLocal.client()));
    }

    public TestLedger(String table) {
        this.client = DynamoDbLocal.client();
        this.table = table;
        this.store = new DynamoLedgerStore(client, table, Duration.ofHours(24));
        this.transactions = new Transactions(store, new Transactions.RetryPolicy(10, Duration.ofMillis(20)));
        this.holds = new DynamoAccountHolds(client, table, Clock.systemUTC());
    }

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
            return new Writes<>(List.of(new PutAccount(change.account()), new PutEntry(change.entry())), change.entry());
        });
    }

    public Payment withdraw(AccountId id, long amount) {
        return withdraw(id, amount, List.of("aml"));
    }

    public Payment withdraw(AccountId id, long amount, List<String> steps) {
        return transactions.onAccount(id, a -> {
            var paymentId = PaymentId.random();
            var change = a.withdraw(usd(amount), paymentId, PARTY, now());
            var payment = Payment.create(paymentId, id, usd(amount), PARTY.id(), steps, now());
            return new Writes<>(
                    List.of(new PutAccount(change.account()), new PutEntry(change.entry()), new PutPayment(payment)),
                    payment);
        });
    }

    public Payment executing(Payment payment) {
        var write = (StepWrite.Recorded) store.recordVerification(payment.id(), "aml", new Verification(true, null, now(), now(), false));
        return new PaymentTransitions(store, transactions, Clock.systemUTC()).markExecuting(write.payment(), Actor.COMPLETION_SERVICE);
    }

    public void block(AccountId id) {
        transactions.onAccount(id, a -> {
            var change = a.block("aml", Actor.AML, now());
            return new Writes<>(List.of(new PutAccount(change.account()), new PutEntry(change.entry())), change.entry());
        });
    }

    public void cancel(Payment payment) {
        transactions.onAccount(payment.accountId(), a -> {
            var change = a.cancelWithdraw(payment.amount(), payment.id(), now());
            return new Writes<>(
                    List.of(new PutAccount(change.account()), new PutEntry(change.entry()),
                            new UpdatePayment(payment.cancel(now()), PaymentStatus.PENDING)),
                    change.entry());
        });
    }

    public Payment payment(PaymentId id) {
        return store.findPayment(id).orElseThrow();
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
