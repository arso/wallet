package org.arso.wallet.executor.adapter.transfer;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.core.IntervalFunction;
import org.arso.wallet.executor.port.TransferService;
import org.arso.wallet.ledger.domain.Payment;

import java.util.concurrent.TimeUnit;

/**
 * Retries an unknown outcome with the same idempotency key (spec 8.5). An open circuit throws
 * {@code CallNotPermittedException} instead of using up attempts, so waiting never leads to NEEDS_REVIEW.
 */
public final class ResilientTransferService implements TransferService {

    private final TransferService delegate;
    private final CircuitBreaker circuit;
    private final int retries;
    private final IntervalFunction backoff;

    public ResilientTransferService(TransferService delegate, CircuitBreaker circuit, int retries, IntervalFunction backoff) {
        this.delegate = delegate;
        this.circuit = circuit;
        this.retries = retries;
        this.backoff = backoff;
    }

    @Override
    public Result transfer(Payment payment) throws InterruptedException {
        var result = call(payment);
        for (int attempt = 1; attempt <= retries && result instanceof Result.Unknown; attempt++) {
            Thread.sleep(backoff.apply(attempt));
            result = call(payment);
        }
        return result;
    }

    private Result call(Payment payment) throws InterruptedException {
        circuit.acquirePermission();
        var start = System.nanoTime();
        try {
            var result = delegate.transfer(payment);
            circuit.onResult(System.nanoTime() - start, TimeUnit.NANOSECONDS, result);
            return result;
        } catch (Throwable e) {
            circuit.releasePermission();
            throw e;
        }
    }
}
