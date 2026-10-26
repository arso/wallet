package org.arso.wallet.timeout.adapter.kafka;

import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.WithdrawRequest;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.ledger.domain.PaymentId;
import org.arso.wallet.ledger.domain.Ulid;
import org.arso.wallet.timeout.application.TimeoutChecker;
import org.arso.wallet.worker.PermanentRecordException;
import org.arso.wallet.worker.RecordHandler;

public final class WithdrawRequestHandler implements RecordHandler {

    private final TimeoutChecker checker;
    private final Duration timeout;

    public WithdrawRequestHandler(TimeoutChecker checker, Duration timeout) {
        this.checker = checker;
        this.timeout = timeout;
    }

    /** Due time of a record; a record that cannot be read is due now and fails in {@link #handle}. */
    public Instant dueAt(ConsumerRecord<String, byte[]> record) {
        return EventJson.read(record.value(), WithdrawRequest.class).requestedAt().plus(timeout);
    }

    @Override
    public Outcome handle(ConsumerRecord<String, byte[]> record) throws InterruptedException {
        var request = EventJson.read(record.value(), WithdrawRequest.class);
        if (request.schemaVersion() != Events.SCHEMA_VERSION) {
            throw new PermanentRecordException("unknown schema_version " + request.schemaVersion());
        }
        if (!Ulid.isValid(request.paymentId())) {
            throw new PermanentRecordException("invalid payment_id " + request.paymentId());
        }
        return checker.check(new PaymentId(request.paymentId()))
                .<Outcome>map(Outcome.RecheckAt::new)
                .orElse(Outcome.DONE);
    }
}
