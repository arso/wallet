package org.arso.wallet.mocks.adapter.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.LedgerEvent;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.mocks.application.AmlMonitor;
import org.arso.wallet.worker.PermanentRecordException;
import org.arso.wallet.worker.RecordHandler;

public final class AmlHandler implements RecordHandler {

    private final AmlMonitor monitor;

    public AmlHandler(AmlMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public Outcome handle(ConsumerRecord<String, byte[]> record) {
        var event = EventJson.read(record.value(), LedgerEvent.class);
        if (event.schemaVersion() != Events.SCHEMA_VERSION) {
            throw new PermanentRecordException("unknown schema_version " + event.schemaVersion());
        }
        monitor.on(event);
        return Outcome.DONE;
    }
}
