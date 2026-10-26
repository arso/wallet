package org.arso.wallet.mocks.config;

import java.time.Duration;
import org.arso.wallet.ledger.port.AccountHolds;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.worker.WorkerPool;
import org.springframework.context.SmartLifecycle;

final class PoolRunner implements SmartLifecycle {

    /** Mocks never write to DynamoDB (spec 8.2): a record that cannot be processed is counted and skipped. */
    static final AccountHolds NO_HOLDS = new AccountHolds() {
        @Override
        public Result holdAccount(String accountId, String reason, String holdRef, Actor actor) {
            return Result.UNATTRIBUTABLE;
        }

        @Override
        public Result holdPaymentAccount(String paymentId, String reason, String holdRef, Actor actor) {
            return Result.UNATTRIBUTABLE;
        }
    };

    private final WorkerPool pool;
    private Thread thread;

    PoolRunner(WorkerPool pool) {
        this.pool = pool;
    }

    @Override
    public void start() {
        thread = Thread.ofPlatform().name("poll-" + pool.hashCode()).start(pool);
    }

    @Override
    public void stop() {
        pool.stop();
        try {
            thread.join(Duration.ofSeconds(30));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return thread != null && thread.isAlive();
    }
}
