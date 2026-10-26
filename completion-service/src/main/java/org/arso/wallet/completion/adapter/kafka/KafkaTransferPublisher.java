package org.arso.wallet.completion.adapter.kafka;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.arso.wallet.completion.port.TransferPublisher;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.TransferRequest;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.ledger.domain.Payment;
import org.arso.wallet.ledger.domain.Ulid;
import org.arso.wallet.worker.AckedProducer;

public final class KafkaTransferPublisher implements TransferPublisher {

    private final AckedProducer producer;

    public KafkaTransferPublisher(AckedProducer producer) {
        this.producer = producer;
    }

    @Override
    public void publish(Payment payment) throws InterruptedException {
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
    }
}
