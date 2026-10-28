package org.arso.wallet.ledger.domain;

import java.security.SecureRandom;
import java.util.regex.Pattern;

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
