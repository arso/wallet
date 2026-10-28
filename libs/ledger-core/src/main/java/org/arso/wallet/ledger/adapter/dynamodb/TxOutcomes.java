package org.arso.wallet.ledger.adapter.dynamodb;

import org.arso.wallet.ledger.port.ConditionFailedException;
import org.arso.wallet.ledger.port.Conflict;
import org.arso.wallet.ledger.port.Conflict.AccountChanged;
import org.arso.wallet.ledger.port.Conflict.DepositExists;
import org.arso.wallet.ledger.port.Conflict.PaymentChanged;
import org.arso.wallet.ledger.port.Conflict.ResponseExists;
import org.arso.wallet.ledger.port.TransientStoreException;
import org.arso.wallet.ledger.port.TxItem;
import org.arso.wallet.ledger.port.TxItem.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import java.util.List;
import java.util.Map;

/**
 * Transaction outcome rules (plan 3.11): condition failures beat bugs, bugs beat retryable reasons.
 */
final class TxOutcomes {

    private TxOutcomes() {
    }

    static RuntimeException classify(TransactionCanceledException e, List<TxItem> items) {
        var reasons = e.cancellationReasons();
        Conflict conflict = null;
        var bestRank = Integer.MAX_VALUE;
        var retry = false;
        var fail = false;
        for (int i = 0; i < reasons.size(); i++) {
            var reason = reasons.get(i);
            switch (reason.code()) {
                case null -> {
                }
                case "None" -> {
                }
                case "ConditionalCheckFailed" -> {
                    var item = items.get(i);
                    if (rank(item) < bestRank) {
                        bestRank = rank(item);
                        conflict = conflict(item, reason.hasItem() ? reason.item() : Map.of());
                    }
                }
                case "TransactionConflict", "ThrottlingError", "ProvisionedThroughputExceeded" -> retry = true;
                default -> fail = true;
            }
        }
        if (conflict != null) {
            return new ConditionFailedException(conflict);
        }
        return !fail && retry ? new TransientStoreException(e) : e;
    }

    private static int rank(TxItem item) {
        return switch (item) {
            case PutResponse _ -> 0;
            case PutDeposit _ -> 1;
            case PutPayment _, UpdatePayment _ -> 2;
            case PutAccount _, CheckAccountActive _ -> 3;
            case PutEntry _ -> 4;
        };
    }

    private static Conflict conflict(TxItem item, Map<String, AttributeValue> old) {
        return switch (item) {
            case PutResponse _ -> new ResponseExists(Items.response(old));
            case PutDeposit _ -> new DepositExists(Items.deposit(old));
            case PutPayment _, UpdatePayment _ -> new PaymentChanged(old.isEmpty() ? null : Items.payment(old));
            case PutAccount _, CheckAccountActive _ -> new AccountChanged(old.isEmpty() ? null : Items.account(old));
            case PutEntry _ -> new AccountChanged(null);
        };
    }
}
