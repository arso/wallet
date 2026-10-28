package org.arso.wallet.events;

import java.util.Objects;

/**
 * Who sent a deposit, as given in the deposit request by the payment provider.
 *
 * @param payerId            the provider's id of the payer
 * @param name               display name, for audit and AML
 * @param accountMasked      masked account number (e.g. {@code ****1234}), safe to show and log
 * @param accountFingerprint HMAC of the full account number, to match the same account without exposing it
 * @param country            country of the account, for AML rules
 */
public record Payer(
        String payerId,
        String name,
        String accountMasked,
        String accountFingerprint,
        String country
) {

    public Payer {
        Objects.requireNonNull(payerId, "payer_id");
    }
}
