package org.arso.wallet.ledger.domain;

import java.time.Instant;
import java.util.Objects;

public sealed interface LedgerEntry {

    record Meta(AccountId accountId, long seq, Instant recordedAt, long balanceAfter, Actor actor, String reason) {

        public Meta {
            Objects.requireNonNull(accountId, "account_id");
            Objects.requireNonNull(recordedAt, "recorded_at");
            Objects.requireNonNull(actor, "actor");
        }
    }

    Meta meta();

    default LedgerEventType type() {
        return switch (this) {
            case AccountCreated _ -> LedgerEventType.ACCOUNT_CREATED;
            case Deposited _ -> LedgerEventType.DEPOSIT;
            case Withdrawn _ -> LedgerEventType.WITHDRAW;
            case WithdrawCompleted _ -> LedgerEventType.WITHDRAW_COMPLETED;
            case UserCancelledWithdraw _ -> LedgerEventType.USER_CANCEL_WITHDRAW;
            case WithdrawRejected _ -> LedgerEventType.REJECT_WITHDRAW;
            case AccountBlocked _ -> LedgerEventType.ACCOUNT_BLOCKED;
            case AccountUnblocked _ -> LedgerEventType.ACCOUNT_UNBLOCKED;
            case AccountHeld _ -> LedgerEventType.ACCOUNT_HELD;
            case AccountReleased _ -> LedgerEventType.ACCOUNT_RELEASED;
        };
    }

    record AccountCreated(Meta meta) implements LedgerEntry {
    }

    record Deposited(Meta meta, Money amount, String externalTxId, Counterparty payer) implements LedgerEntry {

        public Deposited {
            Objects.requireNonNull(externalTxId, "external_tx_id");
        }
    }

    record Withdrawn(Meta meta, Money amount, PaymentId paymentId, Counterparty beneficiary) implements LedgerEntry {
    }

    record WithdrawCompleted(Meta meta, PaymentId paymentId, String externalTxId) implements LedgerEntry {

        public WithdrawCompleted {
            Objects.requireNonNull(externalTxId, "external_tx_id");
        }
    }

    record UserCancelledWithdraw(Meta meta, Money amount, PaymentId paymentId) implements LedgerEntry {
    }

    record WithdrawRejected(Meta meta, Money amount, PaymentId paymentId, String verifier) implements LedgerEntry {

        public WithdrawRejected {
            Objects.requireNonNull(meta.reason(), "reason");
        }

        public RejectReason rejectReason() {
            return RejectReason.valueOf(meta.reason());
        }
    }

    record AccountBlocked(Meta meta) implements LedgerEntry {
    }

    record AccountUnblocked(Meta meta) implements LedgerEntry {
    }

    record AccountHeld(Meta meta, String holdRef, AccountStatus priorStatus) implements LedgerEntry {

        public AccountHeld {
            Objects.requireNonNull(holdRef, "hold_ref");
        }
    }

    record AccountReleased(Meta meta, AccountStatus releasedTo) implements LedgerEntry {
    }
}
