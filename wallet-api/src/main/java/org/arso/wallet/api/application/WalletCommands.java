package org.arso.wallet.api.application;

import org.arso.wallet.api.port.BeneficiaryLookup;
import org.arso.wallet.api.port.JsonWriter;
import org.arso.wallet.ledger.application.Transactions;
import org.arso.wallet.ledger.application.Transactions.Writes;
import org.arso.wallet.ledger.domain.*;
import org.arso.wallet.ledger.domain.LedgerEntry.Deposited;
import org.arso.wallet.ledger.port.*;
import org.arso.wallet.ledger.port.Conflict.DepositExists;
import org.arso.wallet.ledger.port.Conflict.PaymentChanged;
import org.arso.wallet.ledger.port.TxItem.*;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static java.net.HttpURLConnection.*;
import static org.arso.wallet.ledger.domain.DomainException.Code.*;

public final class WalletCommands {

    public record Settings(List<String> verificationSteps, Currency defaultCurrency) {
    }

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
        var response = respond(HTTP_CREATED, ResponseBodies.account(opened.account()));
        transactions.commit(items(opened, idem.put(response, now)));
        return response;
    }

    public ApiResponse deposit(Idem idem, AccountId id, Money amount, String externalTxId, Counterparty payer) {
        try {
            return transactions.onAccount(id, account -> {
                var now = now();
                var change = account.deposit(amount, externalTxId, payer, now);
                var response = respond(HTTP_CREATED, ResponseBodies.deposit((Deposited) change.entry()));
                var dedup = new PutDeposit(new DepositRecord(externalTxId, id, change.entry().meta().seq()), now);
                return new Writes<>(items(change, dedup, idem.put(response, now)), response);
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
            var response = respond(HTTP_ACCEPTED, ResponseBodies.payment(payment));
            return new Writes<>(items(change, new PutPayment(payment), idem.put(response, now)), response);
        });
    }

    public ApiResponse cancel(Idem idem, PaymentId id) {
        var payment = store.findPayment(id).orElseThrow(() -> new DomainException(NOT_FOUND, "unknown payment " + id));
        try {
            var cancelled = transactions.onAccount(payment.accountId(), account -> {
                var now = now();
                var next = payment.cancel(now);
                var change = account.cancelWithdraw(payment.amount(), id, now);
                var response = respond(HTTP_OK, ResponseBodies.payment(next));
                return new Writes<>(
                        items(change, new UpdatePayment(next, PaymentStatus.PENDING), idem.put(response, now)), next);
            });
            ledgerMetrics.finalized(cancelled);
            return respond(HTTP_OK, ResponseBodies.payment(cancelled));
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
            var response = respond(HTTP_OK, ResponseBodies.account(change.account()));
            return new Writes<>(items(change, idem.put(response, now)), response);
        });
    }

    private ApiResponse originalDeposit(Idem idem, AccountId id, DepositRecord original) {
        if (!original.accountId().equals(id)) {
            throw new DomainException(DUPLICATE_DEPOSIT, "external_tx_id already used on another account");
        }
        var entry = (Deposited) store.findEntry(id, original.seq()).orElseThrow();
        var response = respond(HTTP_CREATED, ResponseBodies.deposit(entry));
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
