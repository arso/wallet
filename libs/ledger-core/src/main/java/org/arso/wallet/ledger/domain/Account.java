package org.arso.wallet.ledger.domain;

import org.arso.wallet.ledger.domain.LedgerEntry.*;

import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.arso.wallet.ledger.domain.AccountStatus.*;
import static org.arso.wallet.ledger.domain.DomainException.Code.*;

public record Account(
        AccountId id,
        Money balance,
        AccountStatus status,
        long version,
        AccountStatus preHoldStatus,
        Set<String> holdRefs,
        Instant createdAt,
        Instant updatedAt) {

    public record Change(Account account, LedgerEntry entry) {
    }

    public Account {
        holdRefs = Set.copyOf(holdRefs);
    }

    public static Change open(AccountId id, Currency currency, Instant now) {
        var account = new Account(id, Money.zero(currency), ACTIVE, 1, null, Set.of(), now, now);
        return new Change(account, new AccountCreated(new Meta(id, 1, now, 0, Actor.USER, null)));
    }

    public Change deposit(Money amount, String externalTxId, Counterparty payer, Instant now) {
        requireUsable();
        requirePositive(amount);
        return next(balance.plus(amount), now, Actor.USER, null, m -> new Deposited(m, amount, externalTxId, payer));
    }

    public Change withdraw(Money amount, PaymentId paymentId, Counterparty beneficiary, Instant now) {
        requireUsable();
        requirePositive(amount);
        return next(balance.minus(amount), now, Actor.USER, null, m -> new Withdrawn(m, amount, paymentId, beneficiary));
    }

    public Change cancelWithdraw(Money amount, PaymentId paymentId, Instant now) {
        return next(balance.plus(amount), now, Actor.USER, null, m -> new UserCancelledWithdraw(m, amount, paymentId));
    }

    public Change rejectWithdraw(
            Money amount, PaymentId paymentId, RejectReason reason, String verifier, Actor actor, Instant now) {
        return next(balance.plus(amount), now, actor, reason.name(), m -> new WithdrawRejected(m, amount, paymentId, verifier));
    }

    public Change completeWithdraw(PaymentId paymentId, String externalTxId, Actor actor, Instant now) {
        return next(balance, now, actor, null, m -> new WithdrawCompleted(m, paymentId, externalTxId));
    }

    public Change block(String reason, Actor actor, Instant now) {
        requireNotHeld();
        if (status == BLOCKED) {
            throw new DomainException(INVALID_TRANSITION, "account already blocked");
        }
        return next(balance, BLOCKED, null, Set.of(), now, actor, reason, AccountBlocked::new);
    }

    public Change unblock(String reason, Actor actor, Instant now) {
        requireNotHeld();
        if (status == ACTIVE) {
            throw new DomainException(INVALID_TRANSITION, "account not blocked");
        }
        return next(balance, ACTIVE, null, Set.of(), now, actor, reason, AccountUnblocked::new);
    }

    public Optional<Change> hold(String reason, String holdRef, Actor actor, Instant now) {
        if (holdRefs.contains(holdRef)) {
            return Optional.empty();
        }
        var refs = new HashSet<>(holdRefs);
        refs.add(holdRef);
        var preHold = status == ON_HOLD ? preHoldStatus : status;
        return Optional.of(next(balance, ON_HOLD, preHold, refs, now, actor, reason, m -> new AccountHeld(m, holdRef, status)));
    }

    public Change release(String reason, AccountStatus to, boolean confirm, Actor actor, Instant now) {
        if (status != ON_HOLD) {
            throw new DomainException(ACCOUNT_NOT_ON_HOLD, "account not on hold");
        }
        if (to != ACTIVE && to != BLOCKED) {
            throw new DomainException(INVALID_REQUEST, "release target must be ACTIVE or BLOCKED");
        }
        if (to == ACTIVE && preHoldStatus == BLOCKED && !confirm) {
            throw new DomainException(CONFIRM_REQUIRED, "account was BLOCKED before the hold, confirm required");
        }
        return next(balance, to, null, Set.of(), now, actor, reason, m -> new AccountReleased(m, to));
    }

    private void requireUsable() {
        requireNotHeld();
        if (status == BLOCKED) {
            throw new DomainException(ACCOUNT_BLOCKED, "account blocked");
        }
    }

    private void requireNotHeld() {
        if (status == ON_HOLD) {
            throw new DomainException(ACCOUNT_ON_HOLD, "account on hold");
        }
    }

    private static void requirePositive(Money amount) {
        Money.positive(amount.amountMinor(), amount.currency());
    }

    private Change next(Money newBalance, Instant now, Actor actor, String reason, Function<Meta, LedgerEntry> entry) {
        return next(newBalance, status, preHoldStatus, holdRefs, now, actor, reason, entry);
    }

    private Change next(
            Money newBalance,
            AccountStatus newStatus,
            AccountStatus newPreHoldStatus,
            Set<String> newHoldRefs,
            Instant now,
            Actor actor,
            String reason,
            Function<Meta, LedgerEntry> entry) {
        var account = new Account(id, newBalance, newStatus, version + 1, newPreHoldStatus, newHoldRefs, createdAt, now);
        var meta = new Meta(id, account.version, now, newBalance.amountMinor(), actor, reason);
        return new Change(account, entry.apply(meta));
    }
}
