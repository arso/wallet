package org.arso.wallet.events;

import java.util.Objects;

/**
 * Snapshot of the beneficiary at withdrawal time, fetched by the wallet from the beneficiary service (never sent by the client).
 *
 * @param beneficiaryId      the beneficiary service's id, to look it up again
 * @param name               display name, for audit and AML
 * @param accountMasked      masked account number (e.g. {@code ****1234}), safe to show and log
 * @param accountFingerprint HMAC of the full account number, to match the same account without exposing it
 * @param country            country of the account, for AML rules
 */
public record Beneficiary(
        String beneficiaryId,
        String name,
        String accountMasked,
        String accountFingerprint,
        String country
) {

    public Beneficiary {
        Objects.requireNonNull(beneficiaryId, "beneficiary_id");
    }
}
