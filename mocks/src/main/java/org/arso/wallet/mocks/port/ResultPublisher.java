package org.arso.wallet.mocks.port;

import org.arso.wallet.events.VerificationResult;

public interface ResultPublisher {

    void publish(VerificationResult result) throws InterruptedException;
}
