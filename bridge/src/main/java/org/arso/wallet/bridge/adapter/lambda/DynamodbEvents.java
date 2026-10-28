package org.arso.wallet.bridge.adapter.lambda;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.lambda.runtime.events.transformers.v2.dynamodb.DynamodbAttributeValueTransformer;
import org.arso.wallet.ledger.adapter.dynamodb.LedgerItems;
import org.arso.wallet.ledger.domain.LedgerEntry;

import java.util.List;

public final class DynamodbEvents {

    private DynamodbEvents() {
    }

    public static List<LedgerEntry> ledgerEntries(DynamodbEvent event) {
        return event.getRecords().stream()
                .filter(DynamodbEvents::isLedgerInsert)
                .map(DynamodbEvents::entry)
                .toList();
    }

    private static boolean isLedgerInsert(DynamodbStreamRecord record) {
        return "INSERT".equals(record.getEventName())
                && LedgerItems.isLedgerItem(DynamodbAttributeValueTransformer.toAttributeValueMapV2(record.getDynamodb().getKeys()));
    }

    private static LedgerEntry entry(DynamodbStreamRecord record) {
        var stream = record.getDynamodb();
        try {
            return LedgerItems.entry(DynamodbAttributeValueTransformer.toAttributeValueMapV2(stream.getNewImage()));
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "cannot map ledger item " + stream.getKeys() + " at " + stream.getSequenceNumber(), e);
        }
    }
}
