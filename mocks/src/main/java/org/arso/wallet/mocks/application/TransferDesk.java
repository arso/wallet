package org.arso.wallet.mocks.application;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.arso.wallet.ledger.domain.Ulid;

public final class TransferDesk {

    public sealed interface Result {

        record Sent(String externalTxId) implements Result {}

        record Rejected(String reason) implements Result {}
    }

    private final Map<String, Result> results = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    private final Duration delay;

    public TransferDesk(Duration delay) {
        this.delay = delay;
    }

    public Result transfer(String idempotencyKey, String beneficiaryId) throws InterruptedException {
        var attempt = attempts.computeIfAbsent(idempotencyKey, _ -> new AtomicInteger()).incrementAndGet();
        if (beneficiaryId.startsWith("b-timeout") || (beneficiaryId.startsWith("b-slow") && attempt == 1)) {
            Thread.sleep(delay);
        }
        return results.computeIfAbsent(idempotencyKey, _ -> beneficiaryId.startsWith("b-reject")
                ? new Result.Rejected("rejected by beneficiary bank")
                : new Result.Sent("ts-" + Ulid.next()));
    }

    public int attempts(String idempotencyKey) {
        var count = attempts.get(idempotencyKey);
        return count == null ? 0 : count.get();
    }

    public Result result(String idempotencyKey) {
        return results.get(idempotencyKey);
    }
}
