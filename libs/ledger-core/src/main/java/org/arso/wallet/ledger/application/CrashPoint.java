package org.arso.wallet.ledger.application;

import org.arso.wallet.ledger.domain.PaymentId;

/**
 * Called at named steps (plan phase 1). No-op in production; tests throw here so the record is not marked done and is
 * handled again, like after a crash.
 */
@FunctionalInterface
public interface CrashPoint {

    String AFTER_STEP_WRITE = "after-step-write";
    String AFTER_EXECUTING = "after-executing";
    String AFTER_TRANSFER = "after-transfer";
    String AFTER_REJECT = "after-reject";

    CrashPoint NONE = (_, _) -> {};

    void at(String step, PaymentId payment);
}
