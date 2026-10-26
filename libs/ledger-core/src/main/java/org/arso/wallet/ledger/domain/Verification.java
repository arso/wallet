package org.arso.wallet.ledger.domain;

import java.time.Instant;

public record Verification(boolean allowed, String details, Instant decidedAt, Instant recordedAt, boolean late) {

    public Verification markLate() {
        return new Verification(allowed, details, decidedAt, recordedAt, true);
    }
}
