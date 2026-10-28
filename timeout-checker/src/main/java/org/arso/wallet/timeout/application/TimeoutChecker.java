package org.arso.wallet.timeout.application;

import org.arso.wallet.ledger.application.CrashPoint;
import org.arso.wallet.ledger.application.PaymentTransitions;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.port.LedgerStore;
import org.arso.wallet.timeout.port.TransferPublisher;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.arso.wallet.ledger.application.CrashPoint.AFTER_REJECT;
import static org.arso.wallet.ledger.domain.PaymentStatus.*;
import static org.arso.wallet.ledger.domain.RejectReason.VERIFICATION_TIMEOUT;

/**
 * Verification timeout (spec 8.6) and stuck EXECUTING safety net (plan 5.6). Runs at requested_at + timeout.
 */
public final class TimeoutChecker {

    private final LedgerStore store;
    private final PaymentTransitions transitions;
    private final TransferPublisher publisher;
    private final CrashPoint crash;
    private final Clock clock;
    private final Duration stuckAfter;

    public TimeoutChecker(
            LedgerStore store,
            PaymentTransitions transitions,
            TransferPublisher publisher,
            CrashPoint crash,
            Clock clock,
            Duration stuckAfter) {
        this.store = store;
        this.transitions = transitions;
        this.publisher = publisher;
        this.crash = crash;
        this.clock = clock;
        this.stuckAfter = stuckAfter;
    }

    /**
     * Empty when done; otherwise the time to check the payment again.
     */
    public Optional<Instant> check(PaymentId id) throws InterruptedException {
        var payment = store.findPayment(id).orElse(null);
        if (payment != null && payment.status() == PENDING) {
            payment = transitions.reject(payment, VERIFICATION_TIMEOUT, null, Actor.TIMEOUT_CHECKER);
            if (payment.status() == REJECTED) {
                crash.at(AFTER_REJECT, id);
            }
        }
        if (payment == null || payment.status() != EXECUTING) {
            return Optional.empty();
        }
        var stuckAt = payment.executingAt().plus(stuckAfter);
        if (clock.instant().isBefore(stuckAt)) {
            return Optional.of(stuckAt);
        }
        publisher.republish(payment);
        return Optional.empty();
    }
}
