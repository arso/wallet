package org.arso.wallet.bridge.application;

import org.arso.wallet.bridge.port.EventPublisher.Message;
import org.arso.wallet.events.Beneficiary;
import org.arso.wallet.events.LedgerEvent;
import org.arso.wallet.events.Payer;
import org.arso.wallet.events.WithdrawRequest;
import org.arso.wallet.ledger.domain.Counterparty;
import org.arso.wallet.ledger.domain.LedgerEntry;
import org.arso.wallet.ledger.domain.LedgerEntry.*;
import org.arso.wallet.ledger.domain.Money;

import java.util.List;
import java.util.Map;

import static org.arso.wallet.events.Events.*;

public final class EventMapper {

    private EventMapper() {
    }

    public static List<Message> messages(LedgerEntry entry) {
        var meta = entry.meta();
        var event = ledgerEvent(entry);
        var ledgerMessage = new Message(
                LEDGER_EVENTS,
                meta.accountId().value(),
                event,
                Map.of(CONTENT_TYPE_HEADER, JSON, EVENT_TYPE_HEADER, event.eventType()));
        if (!(entry instanceof Withdrawn w)) {
            return List.of(ledgerMessage);
        }
        var request = new WithdrawRequest(
                SCHEMA_VERSION,
                w.paymentId().value(),
                meta.accountId().value(),
                meta.seq(),
                w.amount().amountMinor(),
                w.amount().currency().code(),
                beneficiary(w.beneficiary()),
                meta.recordedAt());
        return List.of(
                ledgerMessage,
                new Message(WITHDRAW_REQUESTS, w.paymentId().value(), request, Map.of(CONTENT_TYPE_HEADER, JSON)));
    }

    static LedgerEvent ledgerEvent(LedgerEntry entry) {
        Money amount = null;
        String paymentId = null;
        String externalTxId = null;
        Payer payer = null;
        Beneficiary beneficiary = null;
        String verifier = null;
        String holdRef = null;
        String priorStatus = null;
        String releasedTo = null;
        switch (entry) {
            case AccountCreated _, AccountBlocked _, AccountUnblocked _ -> {
            }
            case Deposited d -> {
                amount = d.amount();
                externalTxId = d.externalTxId();
                payer = payer(d.payer());
            }
            case Withdrawn w -> {
                amount = w.amount();
                paymentId = w.paymentId().value();
                beneficiary = beneficiary(w.beneficiary());
            }
            case WithdrawCompleted c -> {
                paymentId = c.paymentId().value();
                externalTxId = c.externalTxId();
            }
            case UserCancelledWithdraw c -> {
                amount = c.amount();
                paymentId = c.paymentId().value();
            }
            case WithdrawRejected r -> {
                amount = r.amount();
                paymentId = r.paymentId().value();
                verifier = r.verifier();
            }
            case AccountHeld h -> {
                holdRef = h.holdRef();
                priorStatus = h.priorStatus().name();
            }
            case AccountReleased r -> releasedTo = r.releasedTo().name();
        }
        var meta = entry.meta();
        return new LedgerEvent(
                SCHEMA_VERSION,
                meta.accountId().value(),
                meta.seq(),
                entry.type().wire(),
                meta.recordedAt(),
                meta.balanceAfter(),
                meta.actor().wire(),
                meta.reason(),
                paymentId,
                amount == null ? null : amount.amountMinor(),
                amount == null ? null : amount.currency().code(),
                externalTxId,
                payer,
                beneficiary,
                verifier,
                holdRef,
                priorStatus,
                releasedTo);
    }

    private static Payer payer(Counterparty c) {
        return new Payer(c.id(), c.name(), c.accountMasked(), c.accountFingerprint(), c.country());
    }

    private static Beneficiary beneficiary(Counterparty c) {
        return new Beneficiary(c.id(), c.name(), c.accountMasked(), c.accountFingerprint(), c.country());
    }
}
