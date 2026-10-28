package org.arso.wallet.timeout.adapter.kafka;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.TransferRequest;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.domain.Ulid;
import org.arso.wallet.timeout.port.TransferPublisher;
import org.arso.wallet.worker.AckedProducer;

import java.nio.charset.StandardCharsets;

public final class KafkaTransferPublisher implements TransferPublisher {

    private final AckedProducer producer;
    private final Counter redriven;

    public KafkaTransferPublisher(AckedProducer producer, MeterRegistry meters) {
        this.producer = producer;
        this.redriven = meters.counter("stuck_executing_redriven_total");
    }

    @Override
    public void republish(Payment payment) throws InterruptedException {
        var request = new TransferRequest(
                Ulid.next(),
                Events.SCHEMA_VERSION,
                payment.id().value(),
                payment.accountId().value(),
                payment.amount().amountMinor(),
                payment.amount().currency().code(),
                payment.beneficiaryId());
        var headers = new RecordHeaders().add(Events.CONTENT_TYPE_HEADER, Events.JSON.getBytes(StandardCharsets.UTF_8));
        producer.sendAndWait(new ProducerRecord<>(
                Events.TRANSFER_REQUESTS, null, request.paymentId(), EventJson.write(request), headers));
        redriven.increment();
    }
}
