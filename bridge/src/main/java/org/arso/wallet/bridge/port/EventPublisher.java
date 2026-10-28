package org.arso.wallet.bridge.port;

import java.util.List;
import java.util.Map;

public interface EventPublisher {

    record Message(String topic, String key, Object payload, Map<String, String> headers) {
    }

    final class PublishFailedException extends RuntimeException {

        public PublishFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Sends all messages and waits for all acks. Throws when any message was not acked.
     */
    void publishAll(List<Message> messages);
}
