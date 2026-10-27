package org.arso.wallet.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.http.HttpStatus;

class AccountFlowsTest extends ApiTest {

    private static final String PAYER = """
            "payer":{"payer_id":"p-1","name":"John","account_masked":"****1234","account_fingerprint":"hmac:1","country":"PL"}""";

    @Test
    void depositWithdrawAndReadLedger() {
        String id = read(post("/accounts", "{\"currency\":\"USD\"}")
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.balance").isEqualTo(0), "$.account_id");

        deposit(id, "k-" + id, 100, "tx-" + id)
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.seq").isEqualTo(2)
                .jsonPath("$.balance_after").isEqualTo(100);

        String paymentId = read(withdraw(id, UUID.randomUUID().toString(), 30)
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.status").isEqualTo("PENDING"), "$.payment_id");
        get("/payments/" + paymentId)
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.account_id").isEqualTo(id)
                .jsonPath("$.required_steps").isEqualTo(List.of("aml", "kyc"))
                .jsonPath("$.received_at").value(String.class,
                        v -> assertThat(v).matches("\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{3}Z"));
        assertThat(balance(id)).isEqualTo(70);

        get("/accounts/" + id + "/ledger?limit=2")
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.entries[*].event_type").isEqualTo(List.of("withdraw", "deposit"))
                .jsonPath("$.entries[0].beneficiary.account_masked").isEqualTo("****9876")
                .jsonPath("$.next_before_seq").isEqualTo(2);
        get("/accounts/" + id + "/ledger?limit=2&before_seq=2")
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.entries[*].event_type").isEqualTo(List.of("account_created"))
                .jsonPath("$.next_before_seq").doesNotExist();
    }

    @Test
    void rejectsPostWithoutIdempotencyKey() {
        assertError(post("/accounts", null, "{}"), HttpStatus.BAD_REQUEST, "invalid_request");
    }

    @Test
    void unknownPathIsProblemDetail() {
        get("/no-such-path")
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.title").isEqualTo("Not Found");
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
        var paymentId = pendingWithdrawal(id, 40);

        post("/payments/" + paymentId + "/cancel", null)
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("CANCELLED");
        assertError(post("/payments/" + paymentId + "/cancel", null), HttpStatus.CONFLICT, "not_cancellable");
        assertThat(balance(id)).isEqualTo(100);
        get("/accounts/" + id + "/ledger")
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.entries[0].event_type").isEqualTo("user_cancel_withdraw");
    }

    @Test
    void blockStopsUserOperationsButNotCancel() {
        var id = account(100);
        var paymentId = pendingWithdrawal(id, 40);

        post("/internal/accounts/" + id + "/block", "{\"reason\":\"aml\"}")
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("BLOCKED");
        assertError(post("/internal/accounts/" + id + "/block", "{\"reason\":\"aml\"}"), HttpStatus.CONFLICT, "invalid_transition");
        assertError(deposit(id, UUID.randomUUID().toString(), 5, "tx-b-" + id), HttpStatus.CONFLICT, "account_blocked");
        assertError(withdraw(id, UUID.randomUUID().toString(), 5), HttpStatus.CONFLICT, "account_blocked");

        post("/payments/" + paymentId + "/cancel", null)
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("CANCELLED");
        assertThat(balance(id)).isEqualTo(100);

        post("/internal/accounts/" + id + "/unblock", "{\"reason\":\"ok\"}").expectStatus().isOk();
        deposit(id, UUID.randomUUID().toString(), 5, "tx-u-" + id).expectStatus().isCreated();
    }

    @Test
    void blockRacingWithdrawalsKeepsBalanceRight() {
        var id = account(100);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            IntStream.range(0, 10).forEach(_ -> executor.submit(() -> withdraw(id, UUID.randomUUID().toString(), 10)));
            executor.submit(() -> post("/internal/accounts/" + id + "/block", "{\"reason\":\"aml\"}"));
        }

        assertThat(status(id)).isEqualTo("BLOCKED");
        assertLedgerMatchesBalance(id);
    }

    void assertLedgerMatchesBalance(String id) {
        List<Map<String, Object>> entries = read(get("/accounts/" + id + "/ledger?limit=200").expectStatus().isOk(), "$.entries");
        long replayed = 0;
        for (var e : entries) {
            var amount = e.get("amount") instanceof Number n ? n.longValue() : 0;
            replayed += switch ((String) e.get("event_type")) {
                case "deposit", "user_cancel_withdraw", "reject_withdraw" -> amount;
                case "withdraw" -> -amount;
                default -> 0;
            };
        }
        assertThat(replayed).isEqualTo(balance(id));
        assertThat(((Number) entries.getFirst().get("seq")).longValue()).isEqualTo(entries.size());
    }
}
