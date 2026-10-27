package org.arso.wallet.testing;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

public final class MetricsAssertions {

    private MetricsAssertions() {}

    /** No account or payment id (ULID) in any label value (spec 15.4). */
    public static void assertNoIdsInLabels(MeterRegistry meters) {
        assertThat(meters.getMeters())
                .flatExtracting(m -> m.getId().getTags())
                .extracting(Tag::getValue)
                .noneMatch(v -> v.matches(".*[0-9A-HJKMNP-TV-Z]{26}.*"));
    }
}
