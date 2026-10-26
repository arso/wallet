package org.arso.wallet.ledger.domain;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class Timestamps {

    private static final DateTimeFormatter FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private Timestamps() {}

    public static String format(Instant instant) {
        return instant == null ? null : FORMAT.format(instant);
    }

    public static Instant parse(String value) {
        return value == null ? null : Instant.parse(value);
    }
}
