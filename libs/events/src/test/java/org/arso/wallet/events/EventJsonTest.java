package org.arso.wallet.events;

import org.arso.wallet.events.json.EventJson;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JacksonException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventJsonTest {

    static final Instant AT = Instant.parse("2026-10-26T12:00:00.120Z");
    static final Payer PAYER = new Payer("p-123", "John Smith", "****1234", "hmac:9f2c", "PL");
    static final Beneficiary BENEFICIARY = new Beneficiary("b-1", "Jane Doe", "****9876", "hmac:77aa", "DE");

    @Test
    void depositRoundTripsWithSpecFieldNames() {
        var event = new LedgerEvent(1, "01JBC3Z8Q4M6T2N9X5R7K1W0HV", 2, "deposit", AT, 10000, "USER", null, null, 10000L,
                "USD", "prov-778812", PAYER, null, null, null, null);

        var json = text(event);

        assertThat(json)
                .contains("\"schema_version\":1", "\"account_id\":\"01JBC3Z8Q4M6T2N9X5R7K1W0HV\"", "\"seq\":2",
                        "\"event_type\":\"deposit\"", "\"recorded_at\":\"2026-10-26T12:00:00.120Z\"",
                        "\"balance_after\":10000", "\"external_tx_id\":\"prov-778812\"", "\"payer_id\":\"p-123\"",
                        "\"account_masked\":\"****1234\"", "\"account_fingerprint\":\"hmac:9f2c\"")
                .doesNotContain("null", "beneficiary", "hold_ref");
        assertThat(roundTrip(event, LedgerEvent.class)).isEqualTo(event);
    }

    @Test
    void holdAndReleaseRoundTrip() {
        var held = new LedgerEvent(1, "01JBC3Z8Q4M6T2N9X5R7K1W0HV", 5, Events.ACCOUNT_HELD, AT, 70, "BRIDGE",
                "RecordTooLargeException", null, null, null, null, null, null, null,
                "ledger/01JBC3Z8Q4M6T2N9X5R7K1W0HV/4", null);
        var released = new LedgerEvent(1, "01JBC3Z8Q4M6T2N9X5R7K1W0HV", 6, Events.ACCOUNT_RELEASED, AT, 70, "INTERNAL",
                "fixed", null, null, null, null, null, null, null, null, "ACTIVE");

        assertThat(text(held)).contains("\"hold_ref\":\"ledger/01JBC3Z8Q4M6T2N9X5R7K1W0HV/4\"")
                .doesNotContain("prior_status");
        assertThat(text(released)).contains("\"released_to\":\"ACTIVE\"");
        assertThat(roundTrip(held, LedgerEvent.class)).isEqualTo(held);
        assertThat(roundTrip(released, LedgerEvent.class)).isEqualTo(released);
    }

    @Test
    void withdrawRequestRoundTrips() {
        var request = new WithdrawRequest(1, "01JBC3ZK7D5E2H8J4M6N9P1R3S", "01JBC3Z8Q4M6T2N9X5R7K1W0HV", 3, 2500, "USD",
                BENEFICIARY, AT);

        assertThat(text(request)).contains("\"requested_at\":\"2026-10-26T12:00:00.120Z\"", "\"beneficiary_id\":\"b-1\"");
        assertThat(roundTrip(request, WithdrawRequest.class)).isEqualTo(request);
    }

    @Test
    void verificationResultRoundTrips() {
        var result = new VerificationResult("01JBC40M2S8X3V5Q7T9Y1Z3B6D", 1, "01JBC3ZK7D5E2H8J4M6N9P1R3S", "aml", false,
                "sanctions list match", Instant.parse("2026-10-26T12:00:03.540Z"));

        assertThat(text(result)).contains("\"event_id\"", "\"allowed\":false", "\"decided_at\":\"2026-10-26T12:00:03.540Z\"");
        assertThat(roundTrip(result, VerificationResult.class)).isEqualTo(result);
    }

    @Test
    void transferRequestRoundTrips() {
        var request = new TransferRequest("01JBC40M2S8X3V5Q7T9Y1Z3B6D", 1, "01JBC3ZK7D5E2H8J4M6N9P1R3S",
                "01JBC3Z8Q4M6T2N9X5R7K1W0HV", 2500, "USD", "b-1");

        assertThat(roundTrip(request, TransferRequest.class)).isEqualTo(request);
    }

    @Test
    void timestampsAlwaysHaveThreeMillisecondDigits() {
        var result = new VerificationResult("e", 1, "p", "aml", true, null, Instant.parse("2026-10-26T12:00:00Z"));

        assertThat(text(result)).contains("\"decided_at\":\"2026-10-26T12:00:00.000Z\"");
    }

    @Test
    void unknownFieldsAreIgnored() {
        var json = """
                {"event_id":"e","schema_version":1,"payment_id":"p","step":"aml","allowed":true,
                 "decided_at":"2026-10-26T12:00:03.540Z","added_later":{"x":1}}""";

        assertThat(EventJson.read(bytes(json), VerificationResult.class).step()).isEqualTo("aml");
    }

    @Test
    void missingRequiredFieldFails() {
        var json = """
                {"event_id":"e","schema_version":1,"step":"aml","allowed":true,"decided_at":"2026-10-26T12:00:03.540Z"}""";

        assertThatThrownBy(() -> EventJson.read(bytes(json), VerificationResult.class))
                .isInstanceOf(JacksonException.class);
    }

    static <T> T roundTrip(T message, Class<T> type) {
        return EventJson.read(EventJson.write(message), type);
    }

    static String text(Object message) {
        return new String(EventJson.write(message), StandardCharsets.UTF_8);
    }

    static byte[] bytes(String json) {
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
