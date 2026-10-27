package org.arso.wallet.ledger.application;

import static org.arso.wallet.ledger.domain.PaymentStatus.EXECUTING;
import static org.arso.wallet.ledger.domain.PaymentStatus.PENDING;
import static org.arso.wallet.ledger.domain.RejectReason.ACCOUNT_BLOCKED;
import static org.arso.wallet.ledger.domain.RejectReason.EXECUTION_FAILED;
import static org.arso.wallet.ledger.domain.RejectReason.VERIFICATION_REJECTED;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Supplier;
import org.arso.wallet.ledger.application.Transactions.Writes;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.domain.DomainException;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.domain.RejectReason;
import org.arso.wallet.ledger.port.ConditionFailedException;
import org.arso.wallet.ledger.port.Conflict.AccountChanged;
import org.arso.wallet.ledger.port.Conflict.PaymentChanged;
import org.arso.wallet.ledger.port.LedgerMetrics;
import org.arso.wallet.ledger.port.LedgerStore;
import org.arso.wallet.ledger.port.TxItem.CheckAccountActive;
import org.arso.wallet.ledger.port.TxItem.PutAccount;
import org.arso.wallet.ledger.port.TxItem.PutEntry;
import org.arso.wallet.ledger.port.TxItem.UpdatePayment;

public final class PaymentTransitions {

    private final LedgerStore store;
    private final Transactions transactions;
    private final Clock clock;
    private final LedgerMetrics metrics;

    public PaymentTransitions(LedgerStore store, Transactions transactions, Clock clock) {
        this(store, transactions, clock, LedgerMetrics.NONE);
    }

    public PaymentTransitions(LedgerStore store, Transactions transactions, Clock clock, LedgerMetrics metrics) {
        this.store = store;
        this.transactions = transactions;
        this.clock = clock;
        this.metrics = metrics;
    }

    public Payment reject(Payment payment, RejectReason reason, String verifier, Actor actor) {
        var expected = reason == EXECUTION_FAILED ? EXECUTING : PENDING;
        if (payment.status() != expected) {
            return payment;
        }
        return stopOnPaymentChange(() -> finalized(transactions.onAccount(payment.accountId(), account -> {
            var now = now();
            var rejected = payment.reject(reason, now);
            var change = account.rejectWithdraw(payment.amount(), payment.id(), reason, verifier, actor, now);
            return new Writes<>(
                    List.of(new PutAccount(change.account()), new PutEntry(change.entry()), new UpdatePayment(rejected, expected)),
                    rejected);
        })));
    }

    public Payment complete(PaymentId id, String externalTxId, Actor actor) {
        var payment = find(id);
        if (payment.status() != EXECUTING) {
            return payment;
        }
        return stopOnPaymentChange(() -> finalized(transactions.onAccount(payment.accountId(), account -> {
            var now = now();
            var completed = payment.complete(externalTxId, now);
            var change = account.completeWithdraw(payment.id(), externalTxId, actor, now);
            return new Writes<>(
                    List.of(new PutAccount(change.account()), new PutEntry(change.entry()), new UpdatePayment(completed, EXECUTING)),
                    completed);
        })));
    }

    public Payment needsReview(PaymentId id) {
        var payment = find(id);
        if (payment.status() != EXECUTING) {
            return payment;
        }
        var reviewed = payment.needsReview(now());
        return stopOnPaymentChange(() -> {
            transactions.commit(List.of(new UpdatePayment(reviewed, EXECUTING)));
            return finalized(reviewed);
        });
    }

    /** Moves a fully allowed PENDING payment to EXECUTING (spec 6.2, 9.8, 9.9). Rejects it when a reject or a block wins. */
    public Payment markExecuting(Payment payment, Actor actor) {
        if (payment.status() != PENDING || !payment.allRequiredAllowed()) {
            return payment;
        }
        var executing = payment.markExecuting(now());
        try {
            transactions.commit(
                    List.of(new CheckAccountActive(payment.accountId()), new UpdatePayment(executing, PENDING, true)));
            metrics.executing(executing);
            return executing;
        } catch (ConditionFailedException e) {
            return switch (e.conflict()) {
                case PaymentChanged(var current) when current != null && current.status() == PENDING && current.anyRejected() ->
                        reject(current, VERIFICATION_REJECTED, firstRejectedStep(current), actor);
                case PaymentChanged(var current) -> current;
                case AccountChanged _ -> reject(payment, ACCOUNT_BLOCKED, null, actor);
                default -> throw e;
            };
        }
    }

    private Payment find(PaymentId id) {
        return store.findPayment(id)
                .orElseThrow(() -> new DomainException(DomainException.Code.NOT_FOUND, "unknown payment " + id));
    }

    private Payment finalized(Payment payment) {
        metrics.finalized(payment);
        return payment;
    }

    private static Payment stopOnPaymentChange(Supplier<Payment> write) {
        try {
            return write.get();
        } catch (ConditionFailedException e) {
            if (e.conflict() instanceof PaymentChanged(var current) && current != null) {
                return current;
            }
            throw e;
        }
    }

    private static String firstRejectedStep(Payment payment) {
        return payment.requiredSteps().stream()
                .filter(s -> payment.verifications().containsKey(s) && !payment.verifications().get(s).allowed())
                .findFirst()
                .orElse(null);
    }

    private Instant now() {
        return clock.instant().truncatedTo(ChronoUnit.MILLIS);
    }
}
