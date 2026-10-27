package org.arso.wallet.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.test.web.servlet.client.RestTestClient.ResponseSpec;

class IdempotencyTest extends ApiTest {

    record Reply(HttpStatusCode status, String body) {}

    @Test
    void sameKeyAndBodyReplaysWithoutSecondOperation() {
        var id = account(0);
        var key = UUID.randomUUID().toString();

        var first = reply(deposit(id, key, 50, "tx-" + key));
        var second = reply(deposit(id, key, 50, "tx-" + key));
        var reordered = reply(post("/accounts/" + id + "/deposits", key, """
                {"payer":{"country":"PL","account_fingerprint":"hmac:1","account_masked":"****1234","name":"John",
                 "payer_id":"p-1"},"external_tx_id":"tx-%s","currency":"USD","amount":50}""".formatted(key)));

        assertThat(second).isEqualTo(first);
        assertThat(reordered).isEqualTo(first);
        assertThat(balance(id)).isEqualTo(50);
    }

    @Test
    void sameKeyWithOtherBodyOrPathIsRejected() {
        var id = account(100);
        var other = account(100);
        var key = UUID.randomUUID().toString();
        withdraw(id, key, 10).expectStatus().isAccepted();

        assertError(withdraw(id, key, 11), HttpStatus.CONFLICT, "idempotency_mismatch");
        assertError(withdraw(other, key, 10), HttpStatus.CONFLICT, "idempotency_mismatch");
        assertThat(balance(id)).isEqualTo(90);
        assertThat(balance(other)).isEqualTo(100);
    }

    @Test
    void concurrentSameKeyRequestsGiveOneOperation() throws Exception {
        var id = account(0);
        var key = UUID.randomUUID().toString();

        var replies = concurrently(10, () -> reply(deposit(id, key, 25, "tx-" + key)));

        assertThat(replies).allSatisfy(r -> assertThat(r).isEqualTo(replies.getFirst()));
        assertThat(replies.getFirst().status()).isEqualTo(HttpStatus.CREATED);
        assertThat(balance(id)).isEqualTo(25);
    }

    @Test
    void concurrentSameKeyWithdrawalsThatDrainTheBalanceAllGetTheWinner() throws Exception {
        var id = account(100);
        var key = UUID.randomUUID().toString();

        var replies = concurrently(10, () -> reply(withdraw(id, key, 100)));

        assertThat(replies).allSatisfy(r -> {
            assertThat(r.status()).isEqualTo(HttpStatus.ACCEPTED);
            assertThat(r).isEqualTo(replies.getFirst());
        });
        assertThat(balance(id)).isZero();
    }

    @Test
    void replayIgnoresLaterStateChanges() {
        var id = account(100);
        var key = UUID.randomUUID().toString();
        var accepted = reply(withdraw(id, key, 60));

        withdraw(id, UUID.randomUUID().toString(), 40).expectStatus().isAccepted();
        assertThat(reply(withdraw(id, key, 60))).isEqualTo(accepted);

        post("/internal/accounts/" + id + "/block", "{\"reason\":\"aml\"}").expectStatus().isOk();
        assertThat(reply(withdraw(id, key, 60))).isEqualTo(accepted);

        BeneficiaryStub.down = true;
        try {
            assertThat(reply(withdraw(id, key, 60))).isEqualTo(accepted);
        } finally {
            BeneficiaryStub.down = false;
        }
    }

    @Test
    void finalErrorsAreStoredButUnavailableIsNot() {
        var id = account(10);
        var tooMuch = UUID.randomUUID().toString();
        assertError(withdraw(id, tooMuch, 50), HttpStatus.UNPROCESSABLE_CONTENT, "insufficient_funds");
        deposit(id, UUID.randomUUID().toString(), 100, "tx-more-" + id).expectStatus().isCreated();
        assertError(withdraw(id, tooMuch, 50), HttpStatus.UNPROCESSABLE_CONTENT, "insufficient_funds");

        var key = UUID.randomUUID().toString();
        BeneficiaryStub.down = true;
        try {
            assertError(withdraw(id, key, 5), HttpStatus.SERVICE_UNAVAILABLE, "beneficiary_unavailable");
        } finally {
            BeneficiaryStub.down = false;
        }
        withdraw(id, key, 5).expectStatus().isAccepted();
    }

    @Test
    void depositRetriedWithNewKeyCreditsOnce() throws Exception {
        var id = account(0);
        var other = account(0);
        var txId = "tx-" + UUID.randomUUID();

        Number seq = read(deposit(id, UUID.randomUUID().toString(), 70, txId).expectStatus().isCreated(), "$.seq");
        deposit(id, UUID.randomUUID().toString(), 70, txId)
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.seq").isEqualTo(seq);
        assertError(deposit(other, UUID.randomUUID().toString(), 70, txId), HttpStatus.CONFLICT, "duplicate_deposit");

        var raceTx = "tx-" + UUID.randomUUID();
        var raced = concurrently(5, () -> reply(deposit(id, UUID.randomUUID().toString(), 10, raceTx)));
        assertThat(raced).allSatisfy(r -> assertThat(r.status()).isEqualTo(HttpStatus.CREATED));

        assertThat(balance(id)).isEqualTo(80);
        assertThat(balance(other)).isZero();
    }

    static Reply reply(ResponseSpec response) {
        var result = response.returnResult(String.class);
        return new Reply(result.getStatus(), result.getResponseBody());
    }

    static <T> List<T> concurrently(int count, Supplier<T> call) throws Exception {
        var futures = new ArrayList<Future<T>>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < count; i++) {
                futures.add(executor.submit(call::get));
            }
        }
        var results = new ArrayList<T>();
        for (var f : futures) {
            results.add(f.get());
        }
        return results;
    }
}
