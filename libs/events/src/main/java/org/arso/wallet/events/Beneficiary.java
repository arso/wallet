package org.arso.wallet.events;

import java.util.Objects;

public record Beneficiary(
        String beneficiaryId, String name, String accountMasked, String accountFingerprint, String country) {

    public Beneficiary {
        Objects.requireNonNull(beneficiaryId, "beneficiary_id");
    }
}
