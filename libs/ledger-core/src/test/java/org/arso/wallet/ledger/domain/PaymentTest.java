package org.arso.wallet.ledger.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.arso.wallet.ledger.domain.AccountTest.NOW;
import static org.arso.wallet.ledger.domain.AccountTest.expect;
import static org.arso.wallet.ledger.domain.DomainException.Code.INVALID_TRANSITION;
import static org.arso.wallet.ledger.domain.DomainException.Code.NOT_CANCELLABLE;
import static org.arso.wallet.ledger.domain.PaymentStatus.*;
import static org.assertj.core.api.Assertions.assertThat;

class PaymentTest {

    static Payment pending() {
        return Payment.create(
                PaymentId.random(), AccountId.random(), AccountTest.usd(10), "b-1", List.of("aml", "kyc"), NOW);
    }

    static Verification verdict(boolean allowed) {
        return new Verification(allowed, null, NOW, NOW, false);
    }

    static Payment allowed() {
        return pending().recordVerification("aml", verdict(true)).recordVerification("kyc", verdict(true));
    }

    @Test
    void executesOnlyWhenAllRequiredStepsAllowed() {
        var partly = pending().recordVerification("aml", verdict(true));

        assertThat(partly.allRequiredAllowed()).isFalse();
        expect(INVALID_TRANSITION, () -> partly.markExecuting(NOW));
        var executing = allowed().markExecuting(NOW);
        assertThat(executing.status()).isEqualTo(EXECUTING);
        assertThat(executing.executingAt()).isEqualTo(NOW);
    }

    @Test
    void executingEndsCompletedOrNeedsReviewOrRejected() {
        var executing = allowed().markExecuting(NOW);

        assertThat(executing.complete("ext-1", NOW).status()).isEqualTo(COMPLETED);
        assertThat(executing.complete("ext-1", NOW).externalTxId()).isEqualTo("ext-1");
        assertThat(executing.needsReview(NOW).status()).isEqualTo(NEEDS_REVIEW);
        assertThat(executing.reject(RejectReason.EXECUTION_FAILED, NOW).status()).isEqualTo(REJECTED);
        expect(INVALID_TRANSITION, () -> executing.reject(RejectReason.VERIFICATION_TIMEOUT, NOW));
        expect(NOT_CANCELLABLE, () -> executing.cancel(NOW));
    }

    @Test
    void pendingCanBeCancelledOrRejected() {
        var pending = pending();

        assertThat(pending.cancel(NOW).status()).isEqualTo(CANCELLED);
        assertThat(pending.reject(RejectReason.VERIFICATION_TIMEOUT, NOW).rejectReason())
                .isEqualTo(RejectReason.VERIFICATION_TIMEOUT);
        expect(INVALID_TRANSITION, () -> pending.reject(RejectReason.EXECUTION_FAILED, NOW));
        expect(INVALID_TRANSITION, () -> pending.complete("x", NOW));
        expect(INVALID_TRANSITION, () -> pending.needsReview(NOW));
    }

    @Test
    void finalStatesAllowNoTransition() {
        var cancelled = pending().cancel(NOW);

        assertThat(cancelled.isFinal()).isTrue();
        expect(NOT_CANCELLABLE, () -> cancelled.cancel(NOW));
        expect(INVALID_TRANSITION, () -> cancelled.reject(RejectReason.VERIFICATION_TIMEOUT, NOW));
        expect(INVALID_TRANSITION, () -> cancelled.markExecuting(NOW));
    }

    @Test
    void verificationIsRecordedOnceAndLateAfterFinal() {
        var rejected = pending().recordVerification("aml", verdict(false));

        assertThat(rejected.anyRejected()).isTrue();
        assertThat(rejected.recordVerification("aml", verdict(true)).verifications().get("aml").allowed()).isFalse();
        var late = rejected.reject(RejectReason.VERIFICATION_REJECTED, NOW).recordVerification("kyc", verdict(true));
        assertThat(late.verifications().get("kyc").late()).isTrue();
        assertThat(pending().recordVerification("kyc", verdict(true)).verifications().get("kyc").late()).isFalse();
    }
}
