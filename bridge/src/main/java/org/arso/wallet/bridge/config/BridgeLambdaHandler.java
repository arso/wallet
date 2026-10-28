package org.arso.wallet.bridge.config;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import org.arso.wallet.bridge.adapter.kafka.KafkaEventPublisher;
import org.arso.wallet.bridge.adapter.lambda.DynamodbEvents;
import org.arso.wallet.bridge.application.Bridge;

import java.util.Map;

// TODO(later): Lambda packaging, event source mapping with FilterCriteria (plan 5.6, 8).
public final class BridgeLambdaHandler implements RequestHandler<DynamodbEvent, Void> {

    private final Bridge bridge;

    public BridgeLambdaHandler() {
        this(new Bridge(KafkaEventPublisher.create(Env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), Map.of())));
    }

    public BridgeLambdaHandler(Bridge bridge) {
        this.bridge = bridge;
    }

    @Override
    public Void handleRequest(DynamodbEvent event, Context context) {
        bridge.publish(DynamodbEvents.ledgerEntries(event));
        return null;
    }
}
