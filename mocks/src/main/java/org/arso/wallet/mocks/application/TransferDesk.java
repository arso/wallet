package org.arso.wallet.mocks.application;

import org.arso.wallet.ledger.domain.Ulid;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TransferDesk {

    public sealed interface Result {

        record Sent(String externalTxId) implements Result {
        }

        record Rejected(String reason) implements Result {
        }
    }

    private final Map<String, Result> results = new ConcurrentHashMap<>();
    private final Duration timeoutDelay;

    public TransferDesk(Duration timeoutDelay) {
        this.timeoutDelay = timeoutDelay;
    }

    public Result transfer(String idempotencyKey, String beneficiaryId) throws InterruptedException {
        if (beneficiaryId.startsWith("b-timeout")) {
            Thread.sleep(timeoutDelay);
        }
        return results.computeIfAbsent(idempotencyKey, _ -> beneficiaryId.startsWith("b-reject")
                ? new Result.Rejected("rejected by beneficiary bank")
                : new Result.Sent("ts-" + Ulid.next()));
    }
}
