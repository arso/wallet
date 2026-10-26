package org.arso.wallet.mocks.adapter.kafka;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.VerificationResult;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.mocks.port.ResultPublisher;
import org.arso.wallet.worker.AckedProducer;

public final class KafkaResultPublisher implements ResultPublisher {

    private final AckedProducer producer;

    public KafkaResultPublisher(AckedProducer producer) {
        this.producer = producer;
    }

    @Override
    public void publish(VerificationResult result) throws InterruptedException {
        var headers = new RecordHeaders().add(Events.CONTENT_TYPE_HEADER, Events.JSON.getBytes(StandardCharsets.UTF_8));
        producer.sendAndWait(new ProducerRecord<>(
                Events.VERIFICATION_RESULTS, null, result.paymentId(), EventJson.write(result), headers));
    }
}
