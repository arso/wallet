package org.arso.wallet.ledger.domain;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.arso.wallet.ledger.domain.DomainException.Code.INVALID_TRANSITION;
import static org.arso.wallet.ledger.domain.DomainException.Code.NOT_CANCELLABLE;
import static org.arso.wallet.ledger.domain.PaymentStatus.*;

public record Payment(
        PaymentId id,
        AccountId accountId,
        Money amount,
        String beneficiaryId,
        PaymentStatus status,
        Instant receivedAt,
        Instant executingAt,
        Instant finalizedAt,
        RejectReason rejectReason,
        String externalTxId,
        List<String> requiredSteps,
        Map<String, Verification> verifications) {

    public Payment {
        requiredSteps = List.copyOf(requiredSteps);
        verifications = Map.copyOf(verifications);
    }

    public static Payment create(
            PaymentId id, AccountId accountId, Money amount, String beneficiaryId, List<String> requiredSteps, Instant receivedAt) {
        return new Payment(
                id, accountId, amount, beneficiaryId, PENDING, receivedAt, null, null, null, null, requiredSteps, Map.of());
    }

    public Payment cancel(Instant now) {
        if (status != PENDING) {
            throw new DomainException(NOT_CANCELLABLE, "payment is " + status);
        }
        return with(CANCELLED, executingAt, now, null, externalTxId);
    }

    public Payment reject(RejectReason reason, Instant now) {
        requireStatus(reason == RejectReason.EXECUTION_FAILED ? EXECUTING : PENDING);
        return with(REJECTED, executingAt, now, reason, externalTxId);
    }

    public Payment markExecuting(Instant now) {
        requireStatus(PENDING);
        if (!allRequiredAllowed()) {
            throw new DomainException(INVALID_TRANSITION, "not all required steps allowed");
        }
        return with(EXECUTING, now, null, null, externalTxId);
    }

    public Payment complete(String newExternalTxId, Instant now) {
        requireStatus(EXECUTING);
        return with(COMPLETED, executingAt, now, null, newExternalTxId);
    }

    public Payment needsReview(Instant now) {
        requireStatus(EXECUTING);
        return with(NEEDS_REVIEW, executingAt, now, null, externalTxId);
    }

    public Payment recordVerification(String step, Verification verification) {
        if (verifications.containsKey(step)) {
            return this;
        }
        var updated = new HashMap<>(verifications);
        updated.put(step, status.isFinal() ? verification.markLate() : verification);
        return new Payment(
                id, accountId, amount, beneficiaryId, status, receivedAt, executingAt, finalizedAt, rejectReason,
                externalTxId, requiredSteps, updated);
    }

    public boolean allRequiredAllowed() {
        return requiredSteps.stream().allMatch(s -> verifications.containsKey(s) && verifications.get(s).allowed());
    }

    public boolean anyRejected() {
        return verifications.values().stream().anyMatch(v -> !v.allowed());
    }

    public boolean isFinal() {
        return status.isFinal();
    }

    private void requireStatus(PaymentStatus expected) {
        if (status != expected) {
            throw new DomainException(INVALID_TRANSITION, "payment is " + status + ", expected " + expected);
        }
    }

    private Payment with(
            PaymentStatus newStatus, Instant newExecutingAt, Instant newFinalizedAt, RejectReason reason, String txId) {
        return new Payment(
                id, accountId, amount, beneficiaryId, newStatus, receivedAt, newExecutingAt, newFinalizedAt, reason, txId,
                requiredSteps, verifications);
    }
}
