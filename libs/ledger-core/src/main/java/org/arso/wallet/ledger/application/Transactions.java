package org.arso.wallet.ledger.application;

import static org.arso.wallet.ledger.domain.DomainException.Code.NOT_FOUND;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.arso.wallet.ledger.domain.Account;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.DomainException;
import org.arso.wallet.ledger.port.ConditionFailedException;
import org.arso.wallet.ledger.port.Conflict.AccountChanged;
import org.arso.wallet.ledger.port.LedgerMetrics;
import org.arso.wallet.ledger.port.LedgerStore;
import org.arso.wallet.ledger.port.TransientStoreException;
import org.arso.wallet.ledger.port.TxItem;

public final class Transactions {

    public record Plan<T>(List<TxItem> items, T result) {

        public static <T> Plan<T> none(T result) {
            return new Plan<>(List.of(), result);
        }
    }

    public record RetryPolicy(int maxAttempts, Duration maxBackoff) {}

    private final LedgerStore store;
    private final RetryPolicy policy;
    private final LedgerMetrics metrics;

    public Transactions(LedgerStore store, RetryPolicy policy) {
        this(store, policy, LedgerMetrics.NONE);
    }

    public Transactions(LedgerStore store, RetryPolicy policy, LedgerMetrics metrics) {
        this.store = store;
        this.policy = policy;
        this.metrics = metrics;
    }

    /**
     * Optimistic locking loop (spec 6.1): read the account, plan the change, commit. A version or seq conflict re-reads
     * and re-plans with a new token; a transient error resends the same items with the same token.
     */
    public <T> T onAccount(AccountId id, Function<Account, Plan<T>> planner) {
        Plan<T> plan = null;
        String token = null;
        for (int attempt = 1; ; attempt++) {
            if (plan == null) {
                var account = store.findAccount(id).orElseThrow(() -> new DomainException(NOT_FOUND, "unknown account " + id));
                plan = planner.apply(account);
                if (plan.items().isEmpty()) {
                    return plan.result();
                }
                token = UUID.randomUUID().toString();
            }
            try {
                store.commit(plan.items(), token);
                return plan.result();
            } catch (ConditionFailedException e) {
                if (!(e.conflict() instanceof AccountChanged)) {
                    throw e;
                }
                metrics.lockRetry();
                plan = null;
            } catch (TransientStoreException _) {
                // same items, same token
            }
            backoff(attempt);
        }
    }

    public void commit(List<TxItem> items) {
        var token = UUID.randomUUID().toString();
        for (int attempt = 1; ; attempt++) {
            try {
                store.commit(items, token);
                return;
            } catch (TransientStoreException _) {
                backoff(attempt);
            }
        }
    }

    private void backoff(int attempt) {
        if (attempt >= policy.maxAttempts()) {
            throw new LockRetriesExhaustedException(attempt);
        }
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(policy.maxBackoff().toMillis() + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LockRetriesExhaustedException(attempt);
        }
    }
}
