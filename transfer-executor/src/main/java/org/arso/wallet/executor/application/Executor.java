package org.arso.wallet.executor.application;

import org.arso.wallet.executor.port.TransferService;
import org.arso.wallet.executor.port.TransferService.Result.Rejected;
import org.arso.wallet.executor.port.TransferService.Result.Sent;
import org.arso.wallet.executor.port.TransferService.Result.Unknown;
import org.arso.wallet.ledger.application.CrashPoint;
import org.arso.wallet.ledger.application.PaymentTransitions;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.port.LedgerStore;

import static org.arso.wallet.ledger.application.CrashPoint.AFTER_REJECT;
import static org.arso.wallet.ledger.application.CrashPoint.AFTER_TRANSFER;
import static org.arso.wallet.ledger.domain.PaymentStatus.EXECUTING;
import static org.arso.wallet.ledger.domain.RejectReason.EXECUTION_FAILED;

/**
 * Transfer executor. Trusts a consistent read of the payment, never the message.
 */
public final class Executor {

    private final LedgerStore store;
    private final PaymentTransitions transitions;
    private final TransferService transfers;
    private final CrashPoint crash;

    public Executor(LedgerStore store, PaymentTransitions transitions, TransferService transfers, CrashPoint crash) {
        this.store = store;
        this.transitions = transitions;
        this.transfers = transfers;
        this.crash = crash;
    }

    public void execute(PaymentId id) throws InterruptedException {
        var payment = store.findPayment(id).orElse(null);
        if (payment == null || payment.status() != EXECUTING) {
            return;
        }
        switch (transfers.transfer(payment)) {
            case Sent(var externalTxId) -> {
                crash.at(AFTER_TRANSFER, id);
                transitions.complete(id, externalTxId, Actor.TRANSFER_EXECUTOR);
            }
            case Rejected _ -> {
                transitions.reject(payment, EXECUTION_FAILED, null, Actor.TRANSFER_EXECUTOR);
                crash.at(AFTER_REJECT, id);
            }
            case Unknown _ -> transitions.needsReview(id);
        }
    }
}
