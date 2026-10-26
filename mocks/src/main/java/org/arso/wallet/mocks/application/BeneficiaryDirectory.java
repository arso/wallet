package org.arso.wallet.mocks.application;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import org.arso.wallet.events.Beneficiary;

public final class BeneficiaryDirectory {

    public Optional<Beneficiary> find(String id) {
        if (id.startsWith("missing")) {
            return Optional.empty();
        }
        var hash = sha256(id);
        var last4 = String.valueOf(Math.floorMod(id.hashCode(), 10_000));
        return Optional.of(new Beneficiary(
                id, "Beneficiary " + id, "****" + "0".repeat(4 - last4.length()) + last4, "hmac:" + hash.substring(0, 16), "DE"));
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
