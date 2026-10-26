package org.arso.wallet.mocks.application;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.VerificationResult;
import org.arso.wallet.events.WithdrawRequest;
import org.arso.wallet.ledger.domain.Ulid;
import org.arso.wallet.mocks.port.ResultPublisher;

public final class Verifiers {

    private final ResultPublisher publisher;
    private final Duration minLatency;
    private final Duration maxLatency;
    private final Clock clock;

    public Verifiers(ResultPublisher publisher, Duration minLatency, Duration maxLatency, Clock clock) {
        this.publisher = publisher;
        this.minLatency = minLatency;
        this.maxLatency = maxLatency;
        this.clock = clock;
    }

    public void verify(String step, WithdrawRequest request) throws InterruptedException {
        var beneficiary = request.beneficiary().beneficiaryId();
        if (beneficiary.contains("silent-" + step)) {
            return;
        }
        Thread.sleep(ThreadLocalRandom.current().nextLong(minLatency.toMillis(), maxLatency.toMillis() + 1));
        var allowed = !beneficiary.contains("deny-" + step);
        publisher.publish(new VerificationResult(
                Ulid.next(),
                Events.SCHEMA_VERSION,
                request.paymentId(),
                step,
                allowed,
                allowed ? null : "mock rule deny-" + step,
                clock.instant()));
    }
}
