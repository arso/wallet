package org.arso.wallet.events;

import java.util.Objects;

public record Payer(String payerId, String name, String accountMasked, String accountFingerprint, String country) {

    public Payer {
        Objects.requireNonNull(payerId, "payer_id");
    }
}
