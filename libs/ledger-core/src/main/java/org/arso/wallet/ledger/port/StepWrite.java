package org.arso.wallet.ledger.port;

import org.arso.wallet.ledger.domain.Payment;

public sealed interface StepWrite {

    record Recorded(Payment payment) implements StepWrite {}

    record Duplicate(Payment payment) implements StepWrite {}

    record Late(Payment payment) implements StepWrite {}

    record NotFound() implements StepWrite {}
}
