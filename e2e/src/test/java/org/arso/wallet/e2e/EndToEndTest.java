package org.arso.wallet.e2e;

import static org.arso.wallet.e2e.Api.awaitFinal;
import static org.arso.wallet.e2e.Api.awaitPayment;
import static org.arso.wallet.e2e.Api.withdrawal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

class EndToEndTest {

    @AfterAll
    static void ledgerEntriesAreNeverChanged() {
        Invariants.checkUnchanged();
    }

    @Test
    void withdrawalRefusedForInsufficientFundsSucceedsAfterTopUp() {
        var account = Api.createAccount();
        assertThat(Api.deposit(account, 100).status()).isEqualTo(201);
        assertThat(status(withdrawal(account, 60, "b-1"))).isEqualTo("COMPLETED");

        var refused = Api.withdraw(account, 80, "b-1");
        assertThat(refused.status()).isEqualTo(422);
        assertThat(refused.text("error")).isEqualTo("insufficient_funds");

        assertThat(Api.deposit(account, 40).status()).isEqualTo(201);
        assertThat(status(withdrawal(account, 80, "b-1"))).isEqualTo("COMPLETED");
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

        assertThat(status(completed)).isEqualTo("COMPLETED");
        assertThat(awaitFinal(transferRejected).path("reject_reason").asString()).isEqualTo("EXECUTION_FAILED");
        assertThat(awaitFinal(verificationRejected).path("reject_reason").asString()).isEqualTo("VERIFICATION_REJECTED");
        assertThat(status(cancelled)).isEqualTo("CANCELLED");
        var late = awaitPayment(verificationRejected,
                p -> p.path("verifications").path("account_verified").path("late").asBoolean());
        assertThat(late.path("status").asString()).isEqualTo("REJECTED");
        assertThat(Api.balance(account)).isEqualTo(1470);
        Invariants.check(account);
    }

    @Test
    void cancelRacingExecutionHasOneWinner() throws InterruptedException {
        var account = Api.account(1000);
        var outcomes = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            var payment = withdrawal(account, 10, "b-1");
            Thread.sleep(Duration.ofMillis(ThreadLocalRandom.current().nextLong(0, 600)));
            var cancel = Api.cancel(payment);
            var status = status(payment);
            assertThat(cancel.status()).as(status).isEqualTo(status.equals("CANCELLED") ? 200 : 409);
            assertThat(status).isIn("CANCELLED", "COMPLETED");
            outcomes.add(status);
        }
        assertThat(Api.balance(account)).isEqualTo(1000 - 10L * outcomes.stream().filter("COMPLETED"::equals).count());
        Invariants.check(account);
    }

    @Test
    void bridgeRepublishingTheWholeStreamHasNoDoubleEffect() throws InterruptedException {
        var account = Api.account(100);
        var completed = withdrawal(account, 30, "b-1");
        var rejected = withdrawal(account, 20, "b-reject-1");
        assertThat(status(completed)).isEqualTo("COMPLETED");
        assertThat(status(rejected)).isEqualTo("REJECTED");
        Invariants.check(account);

        Stack.restartBridge();
        var after = Api.account(100);
        assertThat(status(withdrawal(after, 10, "b-1"))).isEqualTo("COMPLETED");
        Thread.sleep(Duration.ofSeconds(5));

        assertThat(status(completed)).isEqualTo("COMPLETED");
        assertThat(status(rejected)).isEqualTo("REJECTED");
        Invariants.checkUnchanged();
        Invariants.check(account);
        Invariants.check(after);
    }

    @Test
    void executorKilledDuringTransferResumesWithTheSameKey() {
        var account = Api.account(100);
        var id = withdrawal(account, 30, "b-slow-1");
        await().atMost(Duration.ofSeconds(30)).until(() -> Api.transferAttempts(id) >= 1);

        Stack.crashExecutor();

        var payment = awaitFinal(id);
        assertThat(payment.path("status").asString()).isEqualTo("COMPLETED");
        assertThat(Api.transferAttempts(id)).isGreaterThanOrEqualTo(2);
        assertThat(payment.path("external_tx_id").asString())
                .isEqualTo(Api.get(Stack.mocks(), "/transfers/" + id).text("external_tx_id"));
        assertThat(Api.balance(account)).isEqualTo(70);
        Invariants.check(account);
    }

    @Test
    void configChangeMidFlightKeepsTheStoredSteps() {
        var account = Api.account(100);
        var before = withdrawal(account, 10, "b-silent-beneficiary_credibility");
        String after;
        try {
            Stack.restartWalletApi("aml,account_verified");
            after = withdrawal(account, 10, "b-silent-beneficiary_credibility");
        } finally {
            Stack.restartWalletApi(Stack.STEPS);
        }

        var waiting = awaitFinal(before);
        assertThat(waiting.path("required_steps")).hasSize(3);
        assertThat(waiting.path("status").asString()).isEqualTo("REJECTED");
        assertThat(waiting.path("reject_reason").asString()).isEqualTo("VERIFICATION_TIMEOUT");
        var executed = awaitFinal(after);
        assertThat(executed.path("required_steps")).hasSize(2);
        assertThat(executed.path("status").asString()).isEqualTo("COMPLETED");
        Invariants.check(account);
    }

    @Test
    void rebalanceMidProcessingLosesNothingAndDoesNothingTwice() {
        var account = Api.account(10_000);
        var payments = new ArrayList<>(parallel(20, () -> withdrawal(account, 10, "b-1")));
        var completion = Stack.startCompletion();
        var executor = Stack.startExecutor();
        payments.addAll(parallel(20, () -> withdrawal(account, 10, "b-1")));
        Stack.stopGracefully(completion);
        Stack.stopGracefully(executor);
        payments.addAll(parallel(10, () -> withdrawal(account, 10, "b-1")));

        payments.forEach(id -> assertThat(status(id)).as(id).isEqualTo("COMPLETED"));
        assertThat(Api.balance(account)).isEqualTo(10_000 - 50 * 10);
        Invariants.check(account);
    }

    private static String status(String payment) {
        return awaitFinal(payment).path("status").asString();
    }

    private static <T> List<T> parallel(int count, Supplier<T> task) {
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = IntStream.range(0, count).mapToObj(_ -> pool.submit(task::get)).toList();
            pool.shutdown();
            return futures.stream().map(EndToEndTest::join).toList();
        }
    }

    private static <T> T join(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
