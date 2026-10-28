package org.arso.wallet.ledger.domain;

import java.security.SecureRandom;
import java.util.regex.Pattern;

/**
 * ULID (Universally Unique Lexicographically Sortable Identifier) ids for accounts, payments and messages:
 * 48-bit millisecond time, then 80 random bits, in 26 Crockford base32 characters.
 * <p>
 * ULID, not UUID: ids sort by creation time, so an id shows roughly when it was made, and they are shorter (26 chars, no
 * hyphens) and URL-safe. The JDK's {@code UUID.randomUUID()} is fully random (v4) and gives no order.
 * DynamoDB hashes the whole key, so the shared time prefix does not create hot partitions.
 * <p>
 * In house, not a library: the generator is a few lines, the project needs no monotonic or binary forms, and it saves
 * a dependency. Ids made in the same millisecond do not sort among themselves.
 */
public final class Ulid {

    private static final char[] ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();
    private static final Pattern FORMAT = Pattern.compile("[0-7][0-9A-HJKMNP-TV-Z]{25}");
    private static final SecureRandom RANDOM = new SecureRandom();

    private Ulid() {
    }

    public static String next() {
        var out = new char[26];
        var time = System.currentTimeMillis();
        for (int i = 9; i >= 0; i--) {
            out[i] = ALPHABET[(int) (time & 31)];
            time >>>= 5;
        }
        var random = new byte[16];
        RANDOM.nextBytes(random);
        for (int i = 0; i < 16; i++) {
            out[10 + i] = ALPHABET[random[i] & 31];
        }
        return new String(out);
    }

    public static boolean isValid(String value) {
        return value != null && FORMAT.matcher(value).matches();
    }
}
