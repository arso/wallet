package org.arso.wallet.ledger.adapter.dynamodb;

import java.util.Map;
import org.arso.wallet.ledger.domain.LedgerEntry;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class LedgerItems {

    private LedgerItems() {}

    public static boolean isLedgerItem(Map<String, AttributeValue> image) {
        var sk = Items.s(image, "SK");
        return sk != null && sk.startsWith(Items.LEDGER);
    }

    public static LedgerEntry entry(Map<String, AttributeValue> image) {
        return Items.entry(image);
    }
}
