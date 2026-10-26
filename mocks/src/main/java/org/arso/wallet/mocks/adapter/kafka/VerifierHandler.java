package org.arso.wallet.mocks.adapter.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.WithdrawRequest;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.mocks.application.Verifiers;
import org.arso.wallet.worker.PermanentRecordException;
import org.arso.wallet.worker.RecordHandler;

public final class VerifierHandler implements RecordHandler {

    private final String step;
    private final Verifiers verifiers;

    public VerifierHandler(String step, Verifiers verifiers) {
        this.step = step;
        this.verifiers = verifiers;
    }

    @Override
    public Outcome handle(ConsumerRecord<String, byte[]> record) throws InterruptedException {
        var request = EventJson.read(record.value(), WithdrawRequest.class);
        if (request.schemaVersion() != Events.SCHEMA_VERSION) {
            throw new PermanentRecordException("unknown schema_version " + request.schemaVersion());
        }
        verifiers.verify(step, request);
        return Outcome.DONE;
    }
}
