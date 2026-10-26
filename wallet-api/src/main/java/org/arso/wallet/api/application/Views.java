package org.arso.wallet.api.application;

import static org.arso.wallet.ledger.domain.Timestamps.format;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.arso.wallet.ledger.domain.Account;
import org.arso.wallet.ledger.domain.Counterparty;
import org.arso.wallet.ledger.domain.LedgerEntry;
import org.arso.wallet.ledger.domain.LedgerEntry.AccountHeld;
import org.arso.wallet.ledger.domain.LedgerEntry.AccountReleased;
import org.arso.wallet.ledger.domain.LedgerEntry.Deposited;
import org.arso.wallet.ledger.domain.LedgerEntry.UserCancelledWithdraw;
import org.arso.wallet.ledger.domain.LedgerEntry.WithdrawCompleted;
import org.arso.wallet.ledger.domain.LedgerEntry.WithdrawRejected;
import org.arso.wallet.ledger.domain.LedgerEntry.Withdrawn;
import org.arso.wallet.ledger.domain.Money;
import org.arso.wallet.ledger.domain.Payment;

public final class Views {

    private Views() {}

    public record AccountView(String accountId, long balance, String currency, String status, long version) {}

    public record DepositView(
            String accountId, long seq, long amount, String currency, long balanceAfter, String externalTxId) {}

    public record PaymentView(
            String paymentId,
            String accountId,
            long amount,
            String currency,
            String beneficiaryId,
            String status,
            String receivedAt,
            String executingAt,
            String finalizedAt,
            String rejectReason,
            String externalTxId,
            List<String> requiredSteps,
            Map<String, VerificationView> verifications) {}

    public record VerificationView(boolean allowed, String details, String decidedAt, String recordedAt, Boolean late) {}

    public record LedgerPage(List<Map<String, Object>> entries, Long nextBeforeSeq) {}

    public record ErrorView(String error, String message) {}

    static AccountView account(Account a) {
        return new AccountView(
                a.id().value(), a.balance().amountMinor(), a.balance().currency().code(), a.status().name(), a.version());
    }

    static DepositView deposit(Deposited d) {
        return new DepositView(
                d.meta().accountId().value(),
                d.meta().seq(),
                d.amount().amountMinor(),
                d.amount().currency().code(),
                d.meta().balanceAfter(),
                d.externalTxId());
    }

    static PaymentView payment(Payment p) {
        var verifications = new LinkedHashMap<String, VerificationView>();
        p.verifications().forEach((step, v) -> verifications.put(step, new VerificationView(
                v.allowed(), v.details(), format(v.decidedAt()), format(v.recordedAt()), v.late() ? true : null)));
        return new PaymentView(
                p.id().value(),
                p.accountId().value(),
                p.amount().amountMinor(),
                p.amount().currency().code(),
                p.beneficiaryId(),
                p.status().name(),
                format(p.receivedAt()),
                format(p.executingAt()),
                format(p.finalizedAt()),
                p.rejectReason() == null ? null : p.rejectReason().name(),
                p.externalTxId(),
                p.requiredSteps(),
                verifications);
    }

    static LedgerPage ledger(List<LedgerEntry> entries, int limit) {
        var next = entries.size() == limit && entries.getLast().meta().seq() > 1 ? entries.getLast().meta().seq() : null;
        return new LedgerPage(entries.stream().map(Views::entry).toList(), next);
    }

    static Map<String, Object> entry(LedgerEntry e) {
        var m = e.meta();
        var out = new LinkedHashMap<String, Object>();
        out.put("account_id", m.accountId().value());
        out.put("seq", m.seq());
        out.put("event_type", e.type().wire());
        out.put("recorded_at", format(m.recordedAt()));
        out.put("balance_after", m.balanceAfter());
        out.put("actor", m.actor().wire());
        if (m.reason() != null) {
            out.put("reason", m.reason());
        }
        switch (e) {
            case Deposited d -> {
                money(out, d.amount());
                out.put("external_tx_id", d.externalTxId());
                out.put("payer", counterparty(d.payer(), "payer_id"));
            }
            case Withdrawn w -> {
                money(out, w.amount());
                out.put("payment_id", w.paymentId().value());
                out.put("beneficiary", counterparty(w.beneficiary(), "beneficiary_id"));
            }
            case WithdrawCompleted c -> {
                out.put("payment_id", c.paymentId().value());
                out.put("external_tx_id", c.externalTxId());
            }
            case UserCancelledWithdraw c -> {
                money(out, c.amount());
                out.put("payment_id", c.paymentId().value());
            }
            case WithdrawRejected r -> {
                money(out, r.amount());
                out.put("payment_id", r.paymentId().value());
                if (r.verifier() != null) {
                    out.put("verifier", r.verifier());
                }
            }
            case AccountHeld h -> {
                out.put("hold_ref", h.holdRef());
                out.put("prior_status", h.priorStatus().name());
            }
            case AccountReleased r -> out.put("released_to", r.releasedTo().name());
            default -> {}
        }
        return out;
    }

    private static void money(Map<String, Object> out, Money money) {
        out.put("amount", money.amountMinor());
        out.put("currency", money.currency().code());
    }

    private static Map<String, String> counterparty(Counterparty c, String idField) {
        var out = new LinkedHashMap<String, String>();
        out.put(idField, c.id());
        out.put("name", c.name());
        out.put("account_masked", c.accountMasked());
        out.put("account_fingerprint", c.accountFingerprint());
        out.put("country", c.country());
        return out;
    }
}
