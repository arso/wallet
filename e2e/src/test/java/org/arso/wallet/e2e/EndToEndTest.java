package org.arso.wallet.e2e;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static org.arso.wallet.e2e.Api.*;
import static org.assertj.core.api.Assertions.assertThat;

class EndToEndTest {

    @AfterAll
    static void ledgerEntriesAreNeverChanged() {
        Invariants.checkUnchanged();
    }

    @Test
    void withdrawalRefusedForInsufficientFundsSucceedsAfterTopUp() {
        var account = Api.createAccount();
        assertThat(Api.deposit(account, 100).status()).isEqualTo(201);
        assertFinal(withdrawal(account, 60, "b-1"), "COMPLETED");

        var refused = Api.withdraw(account, 80, "b-1");
        assertThat(refused.status()).isEqualTo(422);
        assertThat(refused.text("error")).isEqualTo("insufficient_funds");

        assertThat(Api.deposit(account, 40).status()).isEqualTo(201);
        assertFinal(withdrawal(account, 80, "b-1"), "COMPLETED");
        assertThat(Api.balance(account)).isZero();
        Invariants.check(account);
    }

    @Test
    void oneAccountThroughEveryOutcomeKeepsLedgerAndBalanceInSync() {
        var account = Api.account(1000);
        Api.deposit(account, 500);
        var completed = withdrawal(account, 30, "b-1");
        var transferRejected = withdrawal(account, 20, "b-reject-1");
        var verificationRejected = withdrawal(account, 10, "b-deny-aml-slow-account_verified");
        var cancelled = withdrawal(account, 40, "b-slow-aml");
        assertThat(Api.cancel(cancelled).status()).isEqualTo(200);

        assertFinal(completed, "COMPLETED");
        assertFinal(transferRejected, "REJECTED", "EXECUTION_FAILED");
        assertFinal(verificationRejected, "REJECTED", "VERIFICATION_REJECTED");
        assertFinal(cancelled, "CANCELLED");
        var late = awaitPayment(verificationRejected,
                p -> p.path("verifications").path("account_verified").path("late").asBoolean());
        assertThat(late.path("status").asString()).as(late.toString()).isEqualTo("REJECTED");
        assertThat(Api.balance(account)).isEqualTo(1470);
        Invariants.check(account);
    }

    @Test
    void cancelRacingExecutionHasOneWinner() throws InterruptedException {
        var account = Api.account(1000);
        var completed = 0;
        for (int i = 0; i < 10; i++) {
            var id = withdrawal(account, 10, "b-1");
            Thread.sleep(Duration.ofMillis(ThreadLocalRandom.current().nextLong(0, 600)));
            var cancel = Api.cancel(id);
            var payment = awaitFinal(id);
            var status = payment.path("status").asString();
            assertThat(status).as(payment.toString()).isIn("CANCELLED", "COMPLETED");
            assertThat(cancel.status()).as(payment.toString()).isEqualTo(status.equals("CANCELLED") ? 200 : 409);
            completed += status.equals("COMPLETED") ? 1 : 0;
        }
        assertThat(Api.balance(account)).isEqualTo(1000 - 10L * completed);
        Invariants.check(account);
    }

    private static JsonNode assertFinal(String id, String status) {
        var payment = awaitFinal(id);
        assertThat(payment.path("status").asString()).as(payment.toString()).isEqualTo(status);
        return payment;
    }

    private static void assertFinal(String id, String status, String rejectReason) {
        var payment = assertFinal(id, status);
        assertThat(payment.path("reject_reason").asString()).as(payment.toString()).isEqualTo(rejectReason);
    }
}
