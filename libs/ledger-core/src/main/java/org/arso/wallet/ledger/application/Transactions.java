package org.arso.wallet.ledger.application;

import org.arso.wallet.ledger.domain.Account;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.DomainException;
import org.arso.wallet.ledger.port.*;
import org.arso.wallet.ledger.port.Conflict.AccountChanged;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static org.arso.wallet.ledger.domain.DomainException.Code.NOT_FOUND;

public final class Transactions {

    /**
     * Items to commit in one transaction, and what to return once they are committed.
     */
    public record Writes<T>(List<TxItem> items, T result) {

        public static <T> Writes<T> none(T result) {
            return new Writes<>(List.of(), result);
        }
    }

    public record RetryPolicy(int maxAttempts, Duration maxBackoff) {
    }

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
     * Optimistic locking loop (spec 6.1): read the account, decide the writes, commit. A version or seq conflict re-reads
     * and decides again with a new token; a transient error resends the same items with the same token.
     */
    public <T> T onAccount(AccountId id, Function<Account, Writes<T>> decide) {
        Writes<T> writes = null;
        String token = null;
        for (int attempt = 1; ; attempt++) {
            if (writes == null) {
                var account = store.findAccount(id).orElseThrow(() -> new DomainException(NOT_FOUND, "unknown account " + id));
                writes = decide.apply(account);
                if (writes.items().isEmpty()) {
                    return writes.result();
                }
                token = UUID.randomUUID().toString();
            }
            try {
                store.commit(writes.items(), token);
                return writes.result();
            } catch (ConditionFailedException e) {
                if (!(e.conflict() instanceof AccountChanged)) {
                    throw e;
                }
                metrics.lockRetry();
                writes = null;
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
