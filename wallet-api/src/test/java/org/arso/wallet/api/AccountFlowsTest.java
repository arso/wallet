package org.arso.wallet.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import tools.jackson.databind.JsonNode;

class AccountFlowsTest extends ApiTest {

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
    void rejectsInvalidRequests() {
        var id = account(10);

        assertThat(post("/accounts", null, "{}").status()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertError(post("/accounts", "{\"currency\":\"XYZ1\"}"), HttpStatus.BAD_REQUEST, "invalid_currency");
        assertError(deposit(id, UUID.randomUUID().toString(), 0, "tx-0"), HttpStatus.BAD_REQUEST, "invalid_amount");
        assertError(post("/accounts/" + id + "/deposits", "{not json"), HttpStatus.BAD_REQUEST, "invalid_request");
        assertError(post("/accounts/" + id + "/withdrawals", "{\"amount\":5,\"currency\":\"EUR\",\"beneficiary_id\":\"b-1\"}"),
                HttpStatus.BAD_REQUEST, "currency_mismatch");
        assertError(withdraw(id, UUID.randomUUID().toString(), 11), HttpStatus.UNPROCESSABLE_CONTENT, "insufficient_funds");
        assertError(post("/accounts/" + id + "/withdrawals", "{\"amount\":5,\"beneficiary_id\":\"missing\"}"),
                HttpStatus.NOT_FOUND, "not_found");
        assertError(get("/accounts/01JBC3Z8Q4M6T2N9X5R7K1W0HV"), HttpStatus.NOT_FOUND, "not_found");
        assertError(get("/accounts/not-an-id"), HttpStatus.NOT_FOUND, "not_found");
        assertError(get("/payments/01JBC3Z8Q4M6T2N9X5R7K1W0HV"), HttpStatus.NOT_FOUND, "not_found");
        assertError(get("/accounts/" + id + "/ledger?limit=500"), HttpStatus.BAD_REQUEST, "invalid_request");
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
    }
}
