package org.arso.wallet.completion.application;

import org.arso.wallet.completion.port.CompletionMetrics;
import org.arso.wallet.completion.port.TransferPublisher;
import org.arso.wallet.events.VerificationResult;
import org.arso.wallet.ledger.application.CrashPoint;
import org.arso.wallet.ledger.application.PaymentTransitions;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.domain.Verification;
import org.arso.wallet.ledger.port.LedgerStore;
import org.arso.wallet.ledger.port.StepWrite;

import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.arso.wallet.ledger.application.CrashPoint.*;
import static org.arso.wallet.ledger.domain.PaymentStatus.EXECUTING;
import static org.arso.wallet.ledger.domain.PaymentStatus.REJECTED;
import static org.arso.wallet.ledger.domain.RejectReason.VERIFICATION_REJECTED;

/**
 * Saga coordinator (spec 8.3, plan phase 7). Every decision is taken on the item returned by the step write.
 */
public final class Completion {

    static final int MAX_DETAILS = 100;

    private final LedgerStore store;
    private final PaymentTransitions transitions;
    private final TransferPublisher publisher;
    private final CrashPoint crash;
    private final Clock clock;
    private final CompletionMetrics metrics;

    public Completion(
            LedgerStore store,
            PaymentTransitions transitions,
            TransferPublisher publisher,
            CrashPoint crash,
            Clock clock,
            CompletionMetrics metrics) {
        this.store = store;
        this.transitions = transitions;
        this.publisher = publisher;
        this.crash = crash;
        this.clock = clock;
        this.metrics = metrics;
    }

    public void record(VerificationResult result) throws InterruptedException {
        var id = new PaymentId(result.paymentId());
        var verification = new Verification(
                result.allowed(), cut(result.details()), result.decidedAt(), clock.instant().truncatedTo(ChronoUnit.MILLIS), false);
        switch (store.recordVerification(id, result.step(), verification)) {
            case StepWrite.Recorded(var payment) -> {
                var elapsed = Duration.between(payment.receivedAt(), result.decidedAt());
                metrics.stepDecided(stepLabel(payment, result.step()), elapsed);
                evaluateAfterWrite(payment);
            }
            case StepWrite.Duplicate(var payment) -> evaluateAfterWrite(payment);
            case StepWrite.Late(var payment) -> metrics.lateResult(stepLabel(payment, result.step()));
            case StepWrite.NotFound _ -> {
            }
        }
    }

    private static String stepLabel(Payment payment, String step) {
        return payment.requiredSteps().contains(step) ? step : "other";
    }

    private void evaluateAfterWrite(Payment payment) throws InterruptedException {
        crash.at(AFTER_STEP_WRITE, payment.id());
        evaluate(payment);
    }

    private void evaluate(Payment payment) throws InterruptedException {
        switch (payment.status()) {
            case PENDING -> {
                if (payment.anyRejected()) {
                    transitions.reject(payment, VERIFICATION_REJECTED, rejectedStep(payment), Actor.COMPLETION_SERVICE);
                    crash.at(AFTER_REJECT, payment.id());
                } else if (payment.allRequiredAllowed()) {
                    var next = transitions.markExecuting(payment, Actor.COMPLETION_SERVICE);
                    if (next.status() == EXECUTING) {
                        crash.at(AFTER_EXECUTING, payment.id());
                        publisher.publish(next);
                    } else if (next.status() == REJECTED) {
                        crash.at(AFTER_REJECT, payment.id());
                    }
                }
            }
            case EXECUTING -> publisher.publish(payment);
            default -> {
            }
        }
    }

    private static String rejectedStep(Payment payment) {
        return payment.verifications().entrySet().stream()
                .filter(e -> !e.getValue().allowed())
                .map(Map.Entry::getKey)
                .sorted()
                .findFirst()
                .orElse(null);
    }

    private static String cut(String details) {
        return details == null || details.length() <= MAX_DETAILS ? details : details.substring(0, MAX_DETAILS);
    }
}
