package org.arso.wallet.bridge.config;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import org.arso.wallet.bridge.RecordingPublisher;
import org.arso.wallet.bridge.application.Bridge;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.LedgerEvent;
import org.arso.wallet.ledger.domain.AccountId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.arso.wallet.bridge.RecordingPublisher.ledgerSeqs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BridgeLambdaHandlerTest {

    static final String ACCOUNT = AccountId.random().value();

    final RecordingPublisher publisher = new RecordingPublisher();
    final BridgeLambdaHandler handler = new BridgeLambdaHandler(new Bridge(publisher));

    @Test
    void publishesLedgerInsertsOnly() {
        var event = event(
                record("INSERT", deposit(2)),
                record("MODIFY", deposit(3)),
                record("INSERT", Map.of("PK", s(ACCOUNT), "SK", s("ACCOUNT#"))),
                record("INSERT", deposit(4)));

        handler.handleRequest(event, null);

        assertThat(ledgerSeqs(publisher.sent)).containsExactly(2L, 4L);
        assertThat(publisher.sent.getFirst().payload()).isInstanceOfSatisfying(LedgerEvent.class, e -> {
            assertThat(e.eventType()).isEqualTo("deposit");
            assertThat(e.amount()).isEqualTo(500);
            assertThat(e.payer().payerId()).isEqualTo("p-1");
            assertThat(e.recordedAt()).hasToString("2026-10-26T12:00:00.123Z");
        });
        assertThat(publisher.sent.getFirst().topic()).isEqualTo(Events.LEDGER_EVENTS);
    }

    @Test
    void unmappableItemFailsTheWholeBatch() {
        var broken = new HashMap<>(deposit(3));
        broken.remove("amount");

        assertThatThrownBy(() -> handler.handleRequest(event(record("INSERT", deposit(2)), record("INSERT", broken)), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot map ledger item");
        assertThat(publisher.calls).isEmpty();
    }

    @Test
    void failedPublishFailsTheWholeBatch() {
        publisher.failOnce(RecordingPublisher.ledgerSeq(2));

        assertThatThrownBy(() -> handler.handleRequest(event(record("INSERT", deposit(2))), null))
                .isInstanceOf(org.arso.wallet.bridge.port.EventPublisher.PublishFailedException.class);
    }

    static Map<String, AttributeValue> deposit(long seq) {
        return Map.ofEntries(
                Map.entry("PK", s(ACCOUNT)),
                Map.entry("SK", s("LEDGER#%012d".formatted(seq))),
                Map.entry("account_id", s(ACCOUNT)),
                Map.entry("seq", n(seq)),
                Map.entry("event_type", s("deposit")),
                Map.entry("recorded_at", s("2026-10-26T12:00:00.123Z")),
                Map.entry("balance_after", n(500)),
                Map.entry("actor", s("user")),
                Map.entry("amount", n(500)),
                Map.entry("currency", s("USD")),
                Map.entry("external_tx_id", s("prov-" + seq)),
                Map.entry("payer", new AttributeValue().withM(Map.of(
                        "payer_id", s("p-1"),
                        "name", s("John"),
                        "account_masked", s("****1234"),
                        "account_fingerprint", s("hmac:1"),
                        "country", s("PL")))));
    }

    static DynamodbStreamRecord record(String eventName, Map<String, AttributeValue> image) {
        var record = new DynamodbStreamRecord();
        record.setEventName(eventName);
        record.setDynamodb(new StreamRecord()
                .withKeys(Map.of("PK", image.get("PK"), "SK", image.get("SK")))
                .withNewImage(image)
                .withSequenceNumber("seq-" + image.get("SK").getS()));
        return record;
    }

    static DynamodbEvent event(DynamodbStreamRecord... records) {
        var event = new DynamodbEvent();
        event.setRecords(List.of(records));
        return event;
    }

    static AttributeValue s(String value) {
        return new AttributeValue().withS(value);
    }

    static AttributeValue n(long value) {
        return new AttributeValue().withN(Long.toString(value));
    }
}
