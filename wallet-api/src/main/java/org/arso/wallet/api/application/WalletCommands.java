package org.arso.wallet.api.application;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.arso.wallet.ledger.domain.DomainException.Code.DUPLICATE_DEPOSIT;
import static org.arso.wallet.ledger.domain.DomainException.Code.NOT_CANCELLABLE;
import static org.arso.wallet.ledger.domain.DomainException.Code.NOT_FOUND;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import org.arso.wallet.api.port.BeneficiaryLookup;
import org.arso.wallet.api.port.JsonWriter;
import org.arso.wallet.ledger.application.Transactions;
import org.arso.wallet.ledger.application.Transactions.Plan;
import org.arso.wallet.ledger.domain.Account;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.AccountStatus;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.domain.Counterparty;
import org.arso.wallet.ledger.domain.Currency;
import org.arso.wallet.ledger.domain.DomainException;
import org.arso.wallet.ledger.domain.LedgerEntry.Deposited;
import org.arso.wallet.ledger.domain.Money;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.domain.PaymentStatus;
import org.arso.wallet.ledger.port.ConditionFailedException;
import org.arso.wallet.ledger.port.Conflict.DepositExists;
import org.arso.wallet.ledger.port.Conflict.PaymentChanged;
import org.arso.wallet.ledger.port.DepositRecord;
import org.arso.wallet.ledger.port.LedgerMetrics;
import org.arso.wallet.ledger.port.LedgerStore;
import org.arso.wallet.ledger.port.TxItem;
import org.arso.wallet.ledger.port.TxItem.PutAccount;
import org.arso.wallet.ledger.port.TxItem.PutDeposit;
import org.arso.wallet.ledger.port.TxItem.PutEntry;
import org.arso.wallet.ledger.port.TxItem.PutPayment;
import org.arso.wallet.ledger.port.TxItem.UpdatePayment;

public final class WalletCommands {

    public record Settings(List<String> verificationSteps, Currency defaultCurrency) {}

    private final LedgerStore store;
    private final Transactions transactions;
    private final BeneficiaryLookup beneficiaries;
    private final JsonWriter json;
    private final Clock clock;
    private final Settings settings;
    private final LedgerMetrics ledgerMetrics;

    public WalletCommands(
            LedgerStore store,
            Transactions transactions,
            BeneficiaryLookup beneficiaries,
            JsonWriter json,
            Clock clock,
            Settings settings,
            LedgerMetrics ledgerMetrics) {
        this.store = store;
        this.transactions = transactions;
        this.beneficiaries = beneficiaries;
        this.json = json;
        this.clock = clock;
        this.settings = settings;
        this.ledgerMetrics = ledgerMetrics;
    }

    public ApiResponse createAccount(Idem idem, Currency currency) {
        var now = now();
        var opened = Account.open(AccountId.random(), currency == null ? settings.defaultCurrency() : currency, now);
        var response = respond(HTTP_CREATED, Views.account(opened.account()));
        transactions.commit(items(opened, idem.put(response, now)));
        return response;
    }

    public ApiResponse deposit(Idem idem, AccountId id, Money amount, String externalTxId, Counterparty payer) {
        try {
            return transactions.onAccount(id, account -> {
                var now = now();
                var change = account.deposit(amount, externalTxId, payer, now);
                var response = respond(HTTP_CREATED, Views.deposit((Deposited) change.entry()));
                var dedup = new PutDeposit(new DepositRecord(externalTxId, id, change.entry().meta().seq()), now);
                return new Plan<>(items(change, dedup, idem.put(response, now)), response);
            });
        } catch (ConditionFailedException e) {
            if (e.conflict() instanceof DepositExists(var original)) {
                return originalDeposit(idem, id, original);
            }
            throw e;
        }
    }

    public ApiResponse withdraw(Idem idem, AccountId id, Money amount, String beneficiaryId) {
        var receivedAt = now();
        var beneficiary = beneficiaries.find(beneficiaryId)
                .orElseThrow(() -> new DomainException(NOT_FOUND, "unknown beneficiary " + beneficiaryId));
        return transactions.onAccount(id, account -> {
            var now = now();
            var paymentId = PaymentId.random();
            var change = account.withdraw(amount, paymentId, beneficiary, now);
            var payment = Payment.create(paymentId, id, amount, beneficiaryId, settings.verificationSteps(), receivedAt);
            var response = respond(HTTP_ACCEPTED, Views.payment(payment));
            return new Plan<>(items(change, new PutPayment(payment), idem.put(response, now)), response);
        });
    }

    public ApiResponse cancel(Idem idem, PaymentId id) {
        var payment = store.findPayment(id).orElseThrow(() -> new DomainException(NOT_FOUND, "unknown payment " + id));
        try {
            var cancelled = transactions.onAccount(payment.accountId(), account -> {
                var now = now();
                var next = payment.cancel(now);
                var change = account.cancelWithdraw(payment.amount(), id, now);
                var response = respond(HTTP_OK, Views.payment(next));
                return new Plan<>(
                        items(change, new UpdatePayment(next, PaymentStatus.PENDING), idem.put(response, now)), next);
            });
            ledgerMetrics.finalized(cancelled);
            return respond(HTTP_OK, Views.payment(cancelled));
        } catch (ConditionFailedException e) {
            if (e.conflict() instanceof PaymentChanged) {
                throw new DomainException(NOT_CANCELLABLE, "payment is no longer pending");
            }
            throw e;
        }
    }

    public ApiResponse block(Idem idem, AccountId id, String reason) {
        return changeAccount(idem, id, (account, now) -> account.block(reason, Actor.AML, now));
    }

    public ApiResponse unblock(Idem idem, AccountId id, String reason) {
        return changeAccount(idem, id, (account, now) -> account.unblock(reason, Actor.AML, now));
    }

    public ApiResponse release(Idem idem, AccountId id, AccountStatus to, boolean confirm, String reason) {
        return changeAccount(idem, id, (account, now) -> account.release(reason, to, confirm, Actor.INTERNAL, now));
    }

    private ApiResponse changeAccount(Idem idem, AccountId id, BiFunction<Account, Instant, Account.Change> action) {
        return transactions.onAccount(id, account -> {
            var now = now();
            var change = action.apply(account, now);
            var response = respond(HTTP_OK, Views.account(change.account()));
            return new Plan<>(items(change, idem.put(response, now)), response);
        });
    }

    private ApiResponse originalDeposit(Idem idem, AccountId id, DepositRecord original) {
        if (!original.accountId().equals(id)) {
            throw new DomainException(DUPLICATE_DEPOSIT, "external_tx_id already used on another account");
        }
        var entry = (Deposited) store.findEntry(id, original.seq()).orElseThrow();
        var response = respond(HTTP_CREATED, Views.deposit(entry));
        store.storeResponse(idem.key(), idem.stored(response), now());
        return response;
    }

    private static List<TxItem> items(Account.Change change, TxItem... extra) {
        var items = new ArrayList<TxItem>(List.of(new PutAccount(change.account()), new PutEntry(change.entry())));
        items.addAll(List.of(extra));
        return items;
    }

    private ApiResponse respond(int status, Object body) {
        return new ApiResponse(status, json.write(body));
    }

    private Instant now() {
        return clock.instant().truncatedTo(ChronoUnit.MILLIS);
    }
}
