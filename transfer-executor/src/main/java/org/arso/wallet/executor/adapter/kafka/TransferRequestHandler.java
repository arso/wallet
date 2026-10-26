package org.arso.wallet.executor.adapter.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.TransferRequest;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.executor.application.Executor;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.domain.Ulid;
import org.arso.wallet.worker.PermanentRecordException;
import org.arso.wallet.worker.RecordHandler;

public final class TransferRequestHandler implements RecordHandler {

    private final Executor executor;

    public TransferRequestHandler(Executor executor) {
        this.executor = executor;
    }

    @Override
    public Outcome handle(ConsumerRecord<String, byte[]> record) throws InterruptedException {
        var request = EventJson.read(record.value(), TransferRequest.class);
        if (request.schemaVersion() != Events.SCHEMA_VERSION) {
            throw new PermanentRecordException("unknown schema_version " + request.schemaVersion());
        }
        if (!Ulid.isValid(request.paymentId())) {
            throw new PermanentRecordException("invalid payment_id " + request.paymentId());
        }
        executor.execute(new PaymentId(request.paymentId()));
        return Outcome.DONE;
    }
}
