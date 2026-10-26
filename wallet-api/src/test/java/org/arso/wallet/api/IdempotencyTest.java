package org.arso.wallet.api;

import static org.arso.wallet.api.AccountFlowsTest.assertError;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class IdempotencyTest extends ApiTest {

    @Test
    void sameKeyAndBodyReplaysWithoutSecondOperation() {
        var id = account(0);
        var key = UUID.randomUUID().toString();

        var first = deposit(id, key, 50, "tx-" + key);
        var second = deposit(id, key, 50, "tx-" + key);
        var reordered = post("/accounts/" + id + "/deposits", key, """
                {"payer":{"country":"PL","account_fingerprint":"hmac:1","account_masked":"****1234","name":"John",
                 "payer_id":"p-1"},"external_tx_id":"tx-%s","currency":"USD","amount":50}""".formatted(key));

        assertThat(second).isEqualTo(first);
        assertThat(reordered).isEqualTo(first);
        assertThat(balance(id)).isEqualTo(50);
    }

    @Test
    void sameKeyWithOtherBodyOrPathIsRejected() {
        var id = account(100);
        var other = account(100);
        var key = UUID.randomUUID().toString();
        withdraw(id, key, 10);

        assertError(withdraw(id, key, 11), 409, "idempotency_mismatch");
        assertError(withdraw(other, key, 10), 409, "idempotency_mismatch");
        assertThat(balance(id)).isEqualTo(90);
        assertThat(balance(other)).isEqualTo(100);
    }

    @Test
    void concurrentSameKeyRequestsGiveOneOperation() throws Exception {
        var id = account(0);
        var key = UUID.randomUUID().toString();

        var responses = concurrently(10, () -> deposit(id, key, 25, "tx-" + key));

        assertThat(responses).allSatisfy(r -> assertThat(r).isEqualTo(responses.getFirst()));
        assertThat(responses.getFirst().status()).isEqualTo(201);
        assertThat(balance(id)).isEqualTo(25);
    }

    @Test
    void concurrentSameKeyWithdrawalsThatDrainTheBalanceAllGetTheWinner() throws Exception {
        var id = account(100);
        var key = UUID.randomUUID().toString();

        var responses = concurrently(10, () -> withdraw(id, key, 100));

        assertThat(responses).allSatisfy(r -> {
            assertThat(r.status()).isEqualTo(202);
            assertThat(r).isEqualTo(responses.getFirst());
        });
        assertThat(balance(id)).isZero();
    }

    @Test
    void replayIgnoresLaterStateChanges() {
        var id = account(100);
        var key = UUID.randomUUID().toString();
        var accepted = withdraw(id, key, 60);

        withdraw(id, UUID.randomUUID().toString(), 40);
        assertThat(withdraw(id, key, 60)).isEqualTo(accepted);

        post("/internal/accounts/" + id + "/block", "{\"reason\":\"aml\"}");
        assertThat(withdraw(id, key, 60)).isEqualTo(accepted);

        BeneficiaryStub.down = true;
        try {
            assertThat(withdraw(id, key, 60)).isEqualTo(accepted);
        } finally {
            BeneficiaryStub.down = false;
        }
    }

    @Test
    void finalErrorsAreStoredButUnavailableIsNot() {
        var id = account(10);
        var tooMuch = UUID.randomUUID().toString();
        assertError(withdraw(id, tooMuch, 50), 422, "insufficient_funds");
        deposit(id, UUID.randomUUID().toString(), 100, "tx-more-" + id);
        assertError(withdraw(id, tooMuch, 50), 422, "insufficient_funds");

        var key = UUID.randomUUID().toString();
        BeneficiaryStub.down = true;
        try {
            assertError(withdraw(id, key, 5), 503, "beneficiary_unavailable");
        } finally {
            BeneficiaryStub.down = false;
        }
        assertThat(withdraw(id, key, 5).status()).isEqualTo(202);
    }

    @Test
    void depositRetriedWithNewKeyCreditsOnce() throws Exception {
        var id = account(0);
        var other = account(0);
        var txId = "tx-" + UUID.randomUUID();

        var first = deposit(id, UUID.randomUUID().toString(), 70, txId);
        var retried = deposit(id, UUID.randomUUID().toString(), 70, txId);
        assertThat(retried.number("seq")).isEqualTo(first.number("seq"));
        assertError(deposit(other, UUID.randomUUID().toString(), 70, txId), 409, "duplicate_deposit");

        var raceTx = "tx-" + UUID.randomUUID();
        var raced = concurrently(5, () -> deposit(id, UUID.randomUUID().toString(), 10, raceTx));
        assertThat(raced).allSatisfy(r -> assertThat(r.status()).isEqualTo(201));

        assertThat(balance(id)).isEqualTo(80);
        assertThat(balance(other)).isZero();
    }

    static <T> java.util.List<T> concurrently(int count, Supplier<T> call) throws Exception {
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
