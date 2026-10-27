package org.arso.wallet.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.http.HttpStatus;
import tools.jackson.databind.JsonNode;

class AccountFlowsTest extends ApiTest {

    private static final String PAYER = """
            "payer":{"payer_id":"p-1","name":"John","account_masked":"****1234","account_fingerprint":"hmac:1","country":"PL"}""";

    @Test
    void depositWithdrawAndReadLedger() {
        var created = post("/accounts", "{\"currency\":\"USD\"}");
        assertThat(created.status()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.number("balance")).isZero();
        var id = created.text("account_id");

        var deposited = deposit(id, "k-" + id, 100, "tx-" + id);
        assertThat(deposited.status()).isEqualTo(HttpStatus.CREATED);
        assertThat(deposited.number("seq")).isEqualTo(2);
        assertThat(deposited.number("balance_after")).isEqualTo(100);

        var withdrawn = withdraw(id, UUID.randomUUID().toString(), 30);
        assertThat(withdrawn.status()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(withdrawn.text("status")).isEqualTo("PENDING");
        var payment = get("/payments/" + withdrawn.text("payment_id"));
        assertThat(payment.text("account_id")).isEqualTo(id);
        assertThat(payment.body().path("required_steps").toString()).isEqualTo("[\"aml\",\"kyc\"]");
        assertThat(payment.text("received_at")).matches("\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{3}Z");
        assertThat(balance(id)).isEqualTo(70);

        var first = get("/accounts/" + id + "/ledger?limit=2");
        assertThat(first.body().path("entries").findValuesAsString("event_type")).containsExactly("withdraw", "deposit");
        assertThat(first.body().path("entries").get(0).path("beneficiary").path("account_masked").asString())
                .isEqualTo("****9876");
        assertThat(first.number("next_before_seq")).isEqualTo(2);
        var second = get("/accounts/" + id + "/ledger?limit=2&before_seq=2");
        assertThat(second.body().path("entries").findValuesAsString("event_type")).containsExactly("account_created");
        assertThat(second.body().has("next_before_seq")).isFalse();
    }

    @Test
    void rejectsPostWithoutIdempotencyKey() {
        assertError(post("/accounts", null, "{}"), HttpStatus.BAD_REQUEST, "invalid_request");
    }

    @Test
    void unknownPathIsProblemDetail() {
        var response = get("/no-such-path");
        assertThat(response.status()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.number("status")).isEqualTo(404);
        assertThat(response.text("title")).isEqualTo("Not Found");
    }

    @ParameterizedTest(name = "{0} {1} {2} -> {3} {4}")
    @CsvSource(delimiter = '|', textBlock = """
            POST | /accounts                            | {"currency":"XYZ1"}                                            | 400 | invalid_currency
            POST | /accounts/{id}/deposits              | {not json                                                      | 400 | invalid_request
            POST | /accounts/{id}/deposits              | {"amount":0,"external_tx_id":"tx-1",{payer}}                   | 400 | invalid_amount
            POST | /accounts/{id}/deposits              | {"amount":9223372036854775807,"external_tx_id":"tx-1",{payer}} | 400 | invalid_amount
            POST | /accounts/{id}/withdrawals           | {"amount":1.99,"beneficiary_id":"b-1"}                         | 400 | invalid_request
            POST | /accounts/{id}/withdrawals           | {"amount":"1","beneficiary_id":"b-1"}                          | 400 | invalid_request
            POST | /accounts/{id}/withdrawals           | {"amount":5,"currency":"EUR","beneficiary_id":"b-1"}           | 400 | currency_mismatch
            POST | /accounts/{id}/withdrawals           | {"amount":11,"beneficiary_id":"b-1"}                           | 422 | insufficient_funds
            POST | /accounts/{id}/withdrawals           | {"amount":5,"beneficiary_id":"missing"}                        | 404 | not_found
            GET  | /accounts/01JBC3Z8Q4M6T2N9X5R7K1W0HV |                                                                | 404 | not_found
            GET  | /accounts/not-an-id                  |                                                                | 404 | not_found
            GET  | /payments/01JBC3Z8Q4M6T2N9X5R7K1W0HV |                                                                | 404 | not_found
            GET  | /accounts/{id}/ledger?limit=500      |                                                                | 400 | invalid_request
            """)
    void rejectsInvalidRequest(String method, String path, String body, int status, String error) {
        var url = path.replace("{id}", account(10));
        var response = method.equals("GET") ? get(url) : post(url, body.replace("{payer}", PAYER));
        assertError(response, HttpStatus.valueOf(status), error);
    }

    @Test
    void cancelRefundsOnlyOnce() {
        var id = account(100);
        var paymentId = withdraw(id, UUID.randomUUID().toString(), 40).text("payment_id");

        var cancelled = post("/payments/" + paymentId + "/cancel", null);
        assertThat(cancelled.status()).isEqualTo(HttpStatus.OK);
        assertThat(cancelled.text("status")).isEqualTo("CANCELLED");
        assertError(post("/payments/" + paymentId + "/cancel", null), HttpStatus.CONFLICT, "not_cancellable");
        assertThat(balance(id)).isEqualTo(100);
        assertThat(get("/accounts/" + id + "/ledger").body().path("entries").get(0).path("event_type").asString())
                .isEqualTo("user_cancel_withdraw");
    }

    @Test
    void blockStopsUserOperationsButNotCancel() {
        var id = account(100);
        var paymentId = withdraw(id, UUID.randomUUID().toString(), 40).text("payment_id");

        assertThat(post("/internal/accounts/" + id + "/block", "{\"reason\":\"aml\"}").text("status")).isEqualTo("BLOCKED");
        assertError(post("/internal/accounts/" + id + "/block", "{\"reason\":\"aml\"}"), HttpStatus.CONFLICT, "invalid_transition");
        assertError(deposit(id, UUID.randomUUID().toString(), 5, "tx-b-" + id), HttpStatus.CONFLICT, "account_blocked");
        assertError(withdraw(id, UUID.randomUUID().toString(), 5), HttpStatus.CONFLICT, "account_blocked");

        assertThat(post("/payments/" + paymentId + "/cancel", null).text("status")).isEqualTo("CANCELLED");
        assertThat(balance(id)).isEqualTo(100);

        assertThat(post("/internal/accounts/" + id + "/unblock", "{\"reason\":\"ok\"}").status()).isEqualTo(HttpStatus.OK);
        assertThat(deposit(id, UUID.randomUUID().toString(), 5, "tx-u-" + id).status()).isEqualTo(HttpStatus.CREATED);
    }

    @Test
    void blockRacingWithdrawalsKeepsBalanceRight() throws Exception {
        var id = account(100);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            IntStream.range(0, 10).forEach(_ -> executor.submit(() -> withdraw(id, UUID.randomUUID().toString(), 10)));
            executor.submit(() -> post("/internal/accounts/" + id + "/block", "{\"reason\":\"aml\"}"));
        }

        assertThat(status(id)).isEqualTo("BLOCKED");
        assertLedgerMatchesBalance(id);
    }

    void assertLedgerMatchesBalance(String id) {
        var entries = get("/accounts/" + id + "/ledger?limit=200").body().path("entries");
        long replayed = 0;
        for (JsonNode e : entries) {
            var amount = e.path("amount").asLong();
            replayed += switch (e.path("event_type").asString()) {
                case "deposit", "user_cancel_withdraw", "reject_withdraw" -> amount;
                case "withdraw" -> -amount;
                default -> 0;
            };
        }
        assertThat(replayed).isEqualTo(balance(id));
        assertThat(entries.get(0).path("seq").asLong()).isEqualTo(entries.size());
    }

    static void assertError(Resp response, HttpStatus status, String error) {
        assertThat(response.status()).isEqualTo(status);
        assertThat(response.text("error")).isEqualTo(error);
        assertThat(response.text("type")).isEqualTo("about:blank");
        assertThat(response.number("status")).isEqualTo(status.value());
        assertThat(response.text("title")).isEqualTo(status.getReasonPhrase());
        assertThat(response.body().has("detail")).isTrue();
    }
}
