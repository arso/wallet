package org.arso.wallet.ledger.domain;

import org.arso.wallet.ledger.domain.LedgerEntry.AccountHeld;
import org.arso.wallet.ledger.domain.LedgerEntry.Deposited;
import org.arso.wallet.ledger.domain.LedgerEntry.WithdrawRejected;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.time.Instant;
import java.util.Set;

import static org.arso.wallet.ledger.domain.AccountStatus.*;
import static org.arso.wallet.ledger.domain.DomainException.Code.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AccountTest {

    static final Instant NOW = Instant.parse("2026-10-26T12:00:00.123Z");
    static final Counterparty PARTY = new Counterparty("p-1", "John", "****1234", "hmac:1", "PL");

    static Money usd(long amount) {
        return new Money(amount, Currency.USD);
    }

    static Account funded(long amount) {
        var opened = Account.open(AccountId.random(), Currency.USD, NOW).account();
        return amount == 0 ? opened : opened.deposit(usd(amount), "tx-1", PARTY, NOW).account();
    }

    @Test
    void openStartsAtVersionOneWithZeroBalance() {
        var change = Account.open(AccountId.random(), Currency.USD, NOW);

        assertThat(change.account().version()).isEqualTo(1);
        assertThat(change.account().balance()).isEqualTo(usd(0));
        assertThat(change.entry().meta().seq()).isEqualTo(1);
        assertThat(change.entry().type()).isEqualTo(LedgerEventType.ACCOUNT_CREATED);
    }

    @Test
    void everyChangeBumpsVersionAndUsesItAsSeq() {
        var account = funded(100);
        var change = account.withdraw(usd(30), PaymentId.random(), PARTY, NOW);

        assertThat(change.account().version()).isEqualTo(3);
        assertThat(change.entry().meta().seq()).isEqualTo(3);
        assertThat(change.entry().meta().balanceAfter()).isEqualTo(70);
    }

    @Test
    void depositAddsAmount() {
        var change = funded(0).deposit(usd(50), "tx-2", PARTY, NOW);

        assertThat(change.account().balance()).isEqualTo(usd(50));
        assertThat(change.entry()).isInstanceOf(Deposited.class);
    }

    @Test
    void withdrawNeverGoesNegative() {
        expect(INSUFFICIENT_FUNDS, () -> funded(10).withdraw(usd(11), PaymentId.random(), PARTY, NOW));
    }

    @Test
    void rejectsInvalidAmounts() {
        expect(INVALID_AMOUNT, () -> funded(10).deposit(usd(0), "tx", PARTY, NOW));
        expect(INVALID_AMOUNT, () -> usd(-1));
        expect(CURRENCY_MISMATCH, () -> funded(10).deposit(new Money(5, new Currency("EUR")), "tx", PARTY, NOW));
    }

    @Test
    void blockedAccountRefusesUserOperationsButTakesRefunds() {
        var blocked = funded(100).block("aml", Actor.AML, NOW).account();

        expect(ACCOUNT_BLOCKED, () -> blocked.deposit(usd(1), "tx", PARTY, NOW));
        expect(ACCOUNT_BLOCKED, () -> blocked.withdraw(usd(1), PaymentId.random(), PARTY, NOW));
        var refund = blocked.rejectWithdraw(
                usd(5), PaymentId.random(), RejectReason.ACCOUNT_BLOCKED, null, Actor.COMPLETION_SERVICE, NOW);
        assertThat(refund.account().balance()).isEqualTo(usd(105));
        assertThat(((WithdrawRejected) refund.entry()).rejectReason()).isEqualTo(RejectReason.ACCOUNT_BLOCKED);
        assertThat(blocked.cancelWithdraw(usd(5), PaymentId.random(), NOW).account().balance()).isEqualTo(usd(105));
    }

    @Test
    void blockAndUnblockTransitions() {
        var account = funded(0);
        var blocked = account.block("r", Actor.AML, NOW).account();

        assertThat(blocked.status()).isEqualTo(BLOCKED);
        expect(INVALID_TRANSITION, () -> blocked.block("r", Actor.AML, NOW));
        expect(INVALID_TRANSITION, () -> account.unblock("r", Actor.AML, NOW));
        assertThat(blocked.unblock("r", Actor.AML, NOW).account().status()).isEqualTo(ACTIVE);
    }

    @Test
    void firstHoldKeepsPriorStatusAndRef() {
        var held = funded(10).hold("NullPointerException", "t/0/1", Actor.BRIDGE, NOW).orElseThrow();

        assertThat(held.account().status()).isEqualTo(ON_HOLD);
        assertThat(held.account().preHoldStatus()).isEqualTo(ACTIVE);
        assertThat(held.account().holdRefs()).containsExactly("t/0/1");
        assertThat(((AccountHeld) held.entry()).priorStatus()).isEqualTo(ACTIVE);
    }

    @Test
    void secondHoldAddsRefAndKeepsPreHoldStatus() {
        var blocked = funded(10).block("r", Actor.AML, NOW).account();
        var first = blocked.hold("e", "t/0/1", Actor.BRIDGE, NOW).orElseThrow().account();
        var second = first.hold("e", "t/0/2", Actor.BRIDGE, NOW).orElseThrow();

        assertThat(second.account().preHoldStatus()).isEqualTo(BLOCKED);
        assertThat(second.account().holdRefs()).isEqualTo(Set.of("t/0/1", "t/0/2"));
        assertThat(((AccountHeld) second.entry()).priorStatus()).isEqualTo(ON_HOLD);
        assertThat(second.account().hold("e", "t/0/1", Actor.BRIDGE, NOW)).isEmpty();
    }

    @Test
    void heldAccountRefusesUserOperationsAndBlockChanges() {
        var held = funded(10).hold("e", "ref", Actor.BRIDGE, NOW).orElseThrow().account();

        expect(ACCOUNT_ON_HOLD, () -> held.deposit(usd(1), "tx", PARTY, NOW));
        expect(ACCOUNT_ON_HOLD, () -> held.withdraw(usd(1), PaymentId.random(), PARTY, NOW));
        expect(ACCOUNT_ON_HOLD, () -> held.block("r", Actor.AML, NOW));
        expect(ACCOUNT_ON_HOLD, () -> held.unblock("r", Actor.AML, NOW));
        assertThat(held.cancelWithdraw(usd(5), PaymentId.random(), NOW).account().balance()).isEqualTo(usd(15));
    }

    @Test
    void releaseRules() {
        var active = funded(10);
        var heldFromBlocked = active.block("r", Actor.AML, NOW).account()
                .hold("e", "ref", Actor.BRIDGE, NOW).orElseThrow().account()
                .hold("e", "ref2", Actor.BRIDGE, NOW).orElseThrow().account();

        expect(ACCOUNT_NOT_ON_HOLD, () -> active.release("r", ACTIVE, false, Actor.INTERNAL, NOW));
        expect(CONFIRM_REQUIRED, () -> heldFromBlocked.release("r", ACTIVE, false, Actor.INTERNAL, NOW));

        var released = heldFromBlocked.release("r", ACTIVE, true, Actor.INTERNAL, NOW).account();
        assertThat(released.status()).isEqualTo(ACTIVE);
        assertThat(released.preHoldStatus()).isNull();
        assertThat(released.holdRefs()).isEmpty();
        assertThat(heldFromBlocked.release("r", BLOCKED, false, Actor.INTERNAL, NOW).account().status())
                .isEqualTo(BLOCKED);
    }

    static void expect(DomainException.Code code, Executable action) {
        assertThatThrownBy(action::execute).isInstanceOfSatisfying(DomainException.class, e -> assertThat(e.code())
                .isEqualTo(code));
    }
}
