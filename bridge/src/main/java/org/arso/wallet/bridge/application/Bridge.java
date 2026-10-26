package org.arso.wallet.bridge.application;

import java.util.List;
import org.arso.wallet.bridge.port.EventPublisher;
import org.arso.wallet.ledger.domain.LedgerEntry;

/**
 * Publishes one stream batch. Any failure throws, so the batch is not checkpointed and is sent again; nothing is ever
 * skipped (plan 5.6, "Bridge").
 */
public final class Bridge {

    private final EventPublisher publisher;

    public Bridge(EventPublisher publisher) {
        this.publisher = publisher;
    }

    public void publish(List<LedgerEntry> entries) {
        if (!entries.isEmpty()) {
            publisher.publishAll(entries.stream().flatMap(e -> EventMapper.messages(e).stream()).toList());
        }
    }
}
