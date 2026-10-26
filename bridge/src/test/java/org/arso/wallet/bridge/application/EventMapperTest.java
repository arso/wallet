package org.arso.wallet.bridge.application;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.arso.wallet.events.Beneficiary;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.LedgerEvent;
import org.arso.wallet.events.Payer;
import org.arso.wallet.events.WithdrawRequest;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.AccountStatus;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.domain.Counterparty;
import org.arso.wallet.ledger.domain.Currency;
import org.arso.wallet.ledger.domain.LedgerEntry.AccountHeld;
import org.arso.wallet.ledger.domain.LedgerEntry.AccountReleased;
import org.arso.wallet.ledger.domain.LedgerEntry.Deposited;
import org.arso.wallet.ledger.domain.LedgerEntry.Meta;
import org.arso.wallet.ledger.domain.LedgerEntry.WithdrawRejected;
import org.arso.wallet.ledger.domain.LedgerEntry.Withdrawn;
import org.arso.wallet.ledger.domain.Money;
import org.arso.wallet.ledger.domain.PaymentId;
import org.junit.jupiter.api.Test;

class EventMapperTest {

    static final AccountId ACCOUNT = AccountId.random();
    static final PaymentId PAYMENT = PaymentId.random();
    static final Instant AT = Instant.parse("2026-10-26T12:00:00.123Z");
    static final Counterparty PARTY = new Counterparty("x-1", "John", "****1234", "hmac:1", "PL");

    @Test
    void depositGoesToLedgerEventsOnly() {
        var entry = new Deposited(meta(2, 10000, Actor.USER, null), usd(10000), "prov-1", PARTY);

        var messages = EventMapper.messages(entry);

        assertThat(messages).singleElement().satisfies(m -> {
            assertThat(m.topic()).isEqualTo(Events.LEDGER_EVENTS);
            assertThat(m.key()).isEqualTo(ACCOUNT.value());
            assertThat(m.headers()).containsEntry("event_type", "deposit").containsEntry("content-type", "application/json");
            assertThat(m.payload()).isEqualTo(new LedgerEvent(1, ACCOUNT.value(), 2, "deposit", AT, 10000, "user", null, null,
                    10000L, "USD", "prov-1", new Payer("x-1", "John", "****1234", "hmac:1", "PL"), null, null, null, null,
                    null));
        });
    }

    @Test
    void withdrawGoesToBothTopics() {
        var entry = new Withdrawn(meta(3, 7500, Actor.USER, null), usd(2500), PAYMENT, PARTY);

        var messages = EventMapper.messages(entry);

        assertThat(messages).extracting(m -> m.topic() + "/" + m.key())
                .containsExactly(Events.LEDGER_EVENTS + "/" + ACCOUNT.value(), Events.WITHDRAW_REQUESTS + "/" + PAYMENT.value());
        assertThat(messages.get(1).payload()).isEqualTo(new WithdrawRequest(1, PAYMENT.value(), ACCOUNT.value(), 3, 2500,
                "USD", new Beneficiary("x-1", "John", "****1234", "hmac:1", "PL"), AT));
        assertThat(messages.get(1).headers()).doesNotContainKey("event_type");
    }

    @Test
    void rejectKeepsReasonAndVerifier() {
        var entry = new WithdrawRejected(meta(4, 10000, Actor.COMPLETION_SERVICE, "VERIFICATION_REJECTED"), usd(2500),
                PAYMENT, "aml");

        var event = EventMapper.ledgerEvent(entry);

        assertThat(event.eventType()).isEqualTo("reject_withdraw");
        assertThat(event.actor()).isEqualTo("completion-service");
        assertThat(event.reason()).isEqualTo("VERIFICATION_REJECTED");
        assertThat(event.verifier()).isEqualTo("aml");
        assertThat(event.paymentId()).isEqualTo(PAYMENT.value());
    }

    @Test
    void holdAndReleaseCarryTheirFields() {
        var held = EventMapper.ledgerEvent(
                new AccountHeld(meta(5, 0, Actor.BRIDGE, "RecordTooLargeException"), "ledger/x/4", AccountStatus.BLOCKED));
        var released = EventMapper.ledgerEvent(new AccountReleased(meta(6, 0, Actor.INTERNAL, "fixed"), AccountStatus.ACTIVE));

        assertThat(held.eventType()).isEqualTo(Events.ACCOUNT_HELD);
        assertThat(held.holdRef()).isEqualTo("ledger/x/4");
        assertThat(held.priorStatus()).isEqualTo("BLOCKED");
        assertThat(released.eventType()).isEqualTo(Events.ACCOUNT_RELEASED);
        assertThat(released.releasedTo()).isEqualTo("ACTIVE");
        assertThat(released.amount()).isNull();
    }

    static Meta meta(long seq, long balanceAfter, Actor actor, String reason) {
        return new Meta(ACCOUNT, seq, AT, balanceAfter, actor, reason);
    }

    static Money usd(long amount) {
        return new Money(amount, Currency.USD);
    }
}
