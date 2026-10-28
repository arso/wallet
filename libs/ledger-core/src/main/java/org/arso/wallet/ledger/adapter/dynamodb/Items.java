package org.arso.wallet.ledger.adapter.dynamodb;

import org.arso.wallet.ledger.domain.*;
import org.arso.wallet.ledger.domain.LedgerEntry.*;
import org.arso.wallet.ledger.port.DepositRecord;
import org.arso.wallet.ledger.port.StoredResponse;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.arso.wallet.ledger.domain.Timestamps.format;
import static org.arso.wallet.ledger.domain.Timestamps.parse;

final class Items {

    static final String ACCOUNT = "ACCOUNT#";
    static final String LEDGER = "LEDGER#";
    static final String PAYMENT = "PAYMENT#";
    static final String IDEMPOTENCY = "IDEMPOTENCY#";
    static final String DEPOSIT = "DEPOSIT#";

    private Items() {
    }

    static Map<String, AttributeValue> key(String pk, String sk) {
        return Map.of("PK", str(pk), "SK", str(sk));
    }

    static Map<String, AttributeValue> accountKey(AccountId id) {
        return key(id.value(), ACCOUNT);
    }

    static Map<String, AttributeValue> paymentKey(PaymentId id) {
        return key(PAYMENT + id.value(), PAYMENT);
    }

    static Map<String, AttributeValue> ledgerKey(AccountId id, long seq) {
        return key(id.value(), ledgerSk(seq));
    }

    static Map<String, AttributeValue> responseKey(String key) {
        return key(IDEMPOTENCY + key, IDEMPOTENCY);
    }

    static Map<String, AttributeValue> depositKey(String externalTxId) {
        return key(DEPOSIT + externalTxId, DEPOSIT);
    }

    static String ledgerSk(long seq) {
        return LEDGER + "%012d".formatted(seq);
    }

    static Map<String, AttributeValue> account(Account a) {
        return new Builder(accountKey(a.id()))
                .s("account_id", a.id().value())
                .n("balance", a.balance().amountMinor())
                .s("currency", a.balance().currency().code())
                .s("status", a.status().name())
                .n("version", a.version())
                .s("pre_hold_status", a.preHoldStatus() == null ? null : a.preHoldStatus().name())
                .ss("hold_refs", a.holdRefs())
                .s("created_at", format(a.createdAt()))
                .s("updated_at", format(a.updatedAt()))
                .build();
    }

    static Account account(Map<String, AttributeValue> m) {
        var preHold = s(m, "pre_hold_status");
        var holdRefs = m.containsKey("hold_refs") ? Set.copyOf(m.get("hold_refs").ss()) : Set.<String>of();
        return new Account(
                new AccountId(s(m, "account_id")),
                new Money(n(m, "balance"), new Currency(s(m, "currency"))),
                AccountStatus.valueOf(s(m, "status")),
                n(m, "version"),
                preHold == null ? null : AccountStatus.valueOf(preHold),
                holdRefs,
                parse(s(m, "created_at")),
                parse(s(m, "updated_at")));
    }

    static Map<String, AttributeValue> payment(Payment p) {
        var verifications = p.verifications().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> map(verification(e.getValue()))));
        return new Builder(paymentKey(p.id()))
                .s("payment_id", p.id().value())
                .s("account_id", p.accountId().value())
                .n("amount", p.amount().amountMinor())
                .s("currency", p.amount().currency().code())
                .s("beneficiary_id", p.beneficiaryId())
                .s("status", p.status().name())
                .s("received_at", format(p.receivedAt()))
                .s("executing_at", format(p.executingAt()))
                .s("finalized_at", format(p.finalizedAt()))
                .s("reject_reason", p.rejectReason() == null ? null : p.rejectReason().name())
                .s("external_tx_id", p.externalTxId())
                .put("required_steps", AttributeValue.fromL(p.requiredSteps().stream().map(Items::str).toList()))
                .put("verifications", AttributeValue.fromM(verifications))
                .build();
    }

    static Payment payment(Map<String, AttributeValue> m) {
        var reason = s(m, "reject_reason");
        var verifications = m.get("verifications").m().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> verification(e.getValue().m())));
        return new Payment(
                new PaymentId(s(m, "payment_id")),
                new AccountId(s(m, "account_id")),
                new Money(n(m, "amount"), new Currency(s(m, "currency"))),
                s(m, "beneficiary_id"),
                PaymentStatus.valueOf(s(m, "status")),
                parse(s(m, "received_at")),
                parse(s(m, "executing_at")),
                parse(s(m, "finalized_at")),
                reason == null ? null : RejectReason.valueOf(reason),
                s(m, "external_tx_id"),
                m.get("required_steps").l().stream().map(AttributeValue::s).toList(),
                verifications);
    }

    static Map<String, AttributeValue> verification(Verification v) {
        var b = new Builder(Map.of())
                .put("allowed", AttributeValue.fromBool(v.allowed()))
                .s("details", v.details())
                .s("decided_at", format(v.decidedAt()))
                .s("recorded_at", format(v.recordedAt()));
        return v.late() ? b.put("late", AttributeValue.fromBool(true)).build() : b.build();
    }

    static Verification verification(Map<String, AttributeValue> m) {
        return new Verification(
                m.get("allowed").bool(),
                s(m, "details"),
                parse(s(m, "decided_at")),
                parse(s(m, "recorded_at")),
                m.containsKey("late") && m.get("late").bool());
    }

    static Map<String, AttributeValue> entry(LedgerEntry e) {
        var meta = e.meta();
        var b = new Builder(ledgerKey(meta.accountId(), meta.seq()))
                .s("account_id", meta.accountId().value())
                .n("seq", meta.seq())
                .s("event_type", e.type().wire())
                .s("recorded_at", format(meta.recordedAt()))
                .n("balance_after", meta.balanceAfter())
                .s("actor", meta.actor().wire())
                .s("reason", meta.reason());
        switch (e) {
            case AccountCreated _, AccountBlocked _, AccountUnblocked _ -> {
            }
            case Deposited d -> b.money(d.amount())
                    .s("external_tx_id", d.externalTxId())
                    .put("payer", map(counterparty(d.payer(), "payer_id")));
            case Withdrawn w -> b.money(w.amount())
                    .s("payment_id", w.paymentId().value())
                    .put("beneficiary", map(counterparty(w.beneficiary(), "beneficiary_id")));
            case WithdrawCompleted c -> b.s("payment_id", c.paymentId().value()).s("external_tx_id", c.externalTxId());
            case UserCancelledWithdraw c -> b.money(c.amount()).s("payment_id", c.paymentId().value());
            case WithdrawRejected r ->
                    b.money(r.amount()).s("payment_id", r.paymentId().value()).s("verifier", r.verifier());
            case AccountHeld h -> b.s("hold_ref", h.holdRef()).s("prior_status", h.priorStatus().name());
            case AccountReleased r -> b.s("released_to", r.releasedTo().name());
        }
        return b.build();
    }

    static LedgerEntry entry(Map<String, AttributeValue> m) {
        var meta = new Meta(
                new AccountId(s(m, "account_id")),
                n(m, "seq"),
                parse(s(m, "recorded_at")),
                n(m, "balance_after"),
                Actor.fromWire(s(m, "actor")),
                s(m, "reason"));
        return switch (LedgerEventType.fromWire(s(m, "event_type"))) {
            case ACCOUNT_CREATED -> new AccountCreated(meta);
            case DEPOSIT ->
                    new Deposited(meta, money(m), s(m, "external_tx_id"), counterparty(m.get("payer").m(), "payer_id"));
            case WITHDRAW -> new Withdrawn(
                    meta, money(m), paymentId(m), counterparty(m.get("beneficiary").m(), "beneficiary_id"));
            case WITHDRAW_COMPLETED -> new WithdrawCompleted(meta, paymentId(m), s(m, "external_tx_id"));
            case USER_CANCEL_WITHDRAW -> new UserCancelledWithdraw(meta, money(m), paymentId(m));
            case REJECT_WITHDRAW -> new WithdrawRejected(meta, money(m), paymentId(m), s(m, "verifier"));
            case ACCOUNT_BLOCKED -> new AccountBlocked(meta);
            case ACCOUNT_UNBLOCKED -> new AccountUnblocked(meta);
            case ACCOUNT_HELD -> new AccountHeld(meta, s(m, "hold_ref"), AccountStatus.valueOf(s(m, "prior_status")));
            case ACCOUNT_RELEASED -> new AccountReleased(meta, AccountStatus.valueOf(s(m, "released_to")));
        };
    }

    static Map<String, AttributeValue> response(String key, StoredResponse r, Instant now, Duration ttl) {
        return new Builder(responseKey(key))
                .s("request_hash", r.requestHash())
                .n("response_status", r.status())
                .s("response_body", r.body())
                .s("created_at", format(now))
                .n("ttl", now.plus(ttl).getEpochSecond())
                .build();
    }

    static StoredResponse response(Map<String, AttributeValue> m) {
        return new StoredResponse(s(m, "request_hash"), (int) n(m, "response_status"), s(m, "response_body"));
    }

    static Map<String, AttributeValue> deposit(DepositRecord d, Instant now) {
        return new Builder(depositKey(d.externalTxId()))
                .s("external_tx_id", d.externalTxId())
                .s("account_id", d.accountId().value())
                .n("seq", d.seq())
                .s("created_at", format(now))
                .build();
    }

    static DepositRecord deposit(Map<String, AttributeValue> m) {
        return new DepositRecord(s(m, "external_tx_id"), new AccountId(s(m, "account_id")), n(m, "seq"));
    }

    static AttributeValue str(String value) {
        return AttributeValue.fromS(value);
    }

    static AttributeValue num(long value) {
        return AttributeValue.fromN(Long.toString(value));
    }

    static String s(Map<String, AttributeValue> m, String name) {
        var v = m.get(name);
        return v == null ? null : v.s();
    }

    static long n(Map<String, AttributeValue> m, String name) {
        return Long.parseLong(m.get(name).n());
    }

    private static Map<String, AttributeValue> counterparty(Counterparty c, String idField) {
        return new Builder(Map.of())
                .s(idField, c.id())
                .s("name", c.name())
                .s("account_masked", c.accountMasked())
                .s("account_fingerprint", c.accountFingerprint())
                .s("country", c.country())
                .build();
    }

    private static Counterparty counterparty(Map<String, AttributeValue> m, String idField) {
        return new Counterparty(
                s(m, idField), s(m, "name"), s(m, "account_masked"), s(m, "account_fingerprint"), s(m, "country"));
    }

    private static Money money(Map<String, AttributeValue> m) {
        return new Money(n(m, "amount"), new Currency(s(m, "currency")));
    }

    private static PaymentId paymentId(Map<String, AttributeValue> m) {
        return new PaymentId(s(m, "payment_id"));
    }

    private static AttributeValue map(Map<String, AttributeValue> m) {
        return AttributeValue.fromM(m);
    }

    private static final class Builder {

        private final Map<String, AttributeValue> item;

        Builder(Map<String, AttributeValue> key) {
            item = new HashMap<>(key);
        }

        Builder s(String name, String value) {
            return value == null ? this : put(name, str(value));
        }

        Builder n(String name, long value) {
            return put(name, num(value));
        }

        Builder ss(String name, Set<String> values) {
            return values.isEmpty() ? this : put(name, AttributeValue.fromSs(List.copyOf(values)));
        }

        Builder money(Money money) {
            return n("amount", money.amountMinor()).s("currency", money.currency().code());
        }

        Builder put(String name, AttributeValue value) {
            item.put(name, value);
            return this;
        }

        Map<String, AttributeValue> build() {
            return item;
        }
    }
}
