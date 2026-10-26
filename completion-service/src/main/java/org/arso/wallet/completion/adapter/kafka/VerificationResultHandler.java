package org.arso.wallet.completion.adapter.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.arso.wallet.completion.application.Completion;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.VerificationResult;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.ledger.domain.Ulid;
import org.arso.wallet.worker.PermanentRecordException;
import org.arso.wallet.worker.RecordHandler;

public final class VerificationResultHandler implements RecordHandler {

    private final Completion completion;

    public VerificationResultHandler(Completion completion) {
        this.completion = completion;
    }

    @Override
    public Outcome handle(ConsumerRecord<String, byte[]> record) throws InterruptedException {
        var result = EventJson.read(record.value(), VerificationResult.class);
        if (result.schemaVersion() != Events.SCHEMA_VERSION) {
            throw new PermanentRecordException("unknown schema_version " + result.schemaVersion());
        }
        if (!Ulid.isValid(result.paymentId())) {
            throw new PermanentRecordException("invalid payment_id " + result.paymentId());
        }
        completion.record(result);
        return Outcome.DONE;
    }
}
