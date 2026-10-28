package org.arso.wallet.ledger.adapter.dynamodb;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

// TODO: table is created from code for tests and local runs only; move to IaC for real environments (plan phase 2).
public final class TableSetup {

    private TableSetup() {
    }

    public static void createIfMissing(DynamoDbClient client, String table) {
        try {
            client.createTable(b -> b.tableName(table)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(
                            a -> a.attributeName("PK").attributeType(ScalarAttributeType.S),
                            a -> a.attributeName("SK").attributeType(ScalarAttributeType.S))
                    .keySchema(
                            k -> k.attributeName("PK").keyType(KeyType.HASH),
                            k -> k.attributeName("SK").keyType(KeyType.RANGE))
                    .streamSpecification(s -> s.streamEnabled(true).streamViewType(StreamViewType.NEW_IMAGE)));
        } catch (ResourceInUseException e) {
            return;
        }
        client.waiter().waitUntilTableExists(b -> b.tableName(table));
        client.updateTimeToLive(b -> b.tableName(table)
                .timeToLiveSpecification(t -> t.attributeName("ttl").enabled(true)));
    }
}
