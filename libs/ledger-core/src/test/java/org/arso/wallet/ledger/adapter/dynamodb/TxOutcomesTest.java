package org.arso.wallet.ledger.adapter.dynamodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.arso.wallet.ledger.domain.Account;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.Currency;
import org.arso.wallet.ledger.port.ConditionFailedException;
import org.arso.wallet.ledger.port.Conflict.AccountChanged;
import org.arso.wallet.ledger.port.Conflict.ResponseExists;
import org.arso.wallet.ledger.port.StoredResponse;
import org.arso.wallet.ledger.port.TransientStoreException;
import org.arso.wallet.ledger.port.TxItem;
import org.arso.wallet.ledger.port.TxItem.PutAccount;
import org.arso.wallet.ledger.port.TxItem.PutEntry;
import org.arso.wallet.ledger.port.TxItem.PutResponse;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

class TxOutcomesTest {

    static final Instant NOW = Instant.parse("2026-10-26T12:00:00.000Z");
    static final Account.Change OPENED = Account.open(AccountId.random(), Currency.USD, NOW);
    static final StoredResponse STORED = new StoredResponse("hash", 202, "{}");
    static final List<TxItem> ITEMS = List.of(
            new PutAccount(OPENED.account()), new PutEntry(OPENED.entry()), new PutResponse("key", STORED, NOW));

    @Test
    void idempotencyWinsOverAccountAndLedgerConditions() {
        var result = classify(
                failed(Items.account(OPENED.account())),
                failed(Map.of()),
                failed(Items.response("key", STORED, NOW, Duration.ofHours(24))));

        assertThat(result).isInstanceOfSatisfying(ConditionFailedException.class, e -> assertThat(e.conflict())
                .isEqualTo(new ResponseExists(STORED)));
    }

    @Test
    void conditionFailureWinsOverConflict() {
        var result = classify(reason("TransactionConflict"), failed(Map.of()), reason("None"));

        assertThat(result).isInstanceOfSatisfying(ConditionFailedException.class, e -> assertThat(e.conflict())
                .isEqualTo(new AccountChanged(null)));
    }

    @Test
    void conflictAndThrottlingAreTransient() {
        assertThat(classify(reason("TransactionConflict"), reason("None"), reason("None")))
                .isInstanceOf(TransientStoreException.class);
        assertThat(classify(reason("None"), reason("ThrottlingError"), reason("None")))
                .isInstanceOf(TransientStoreException.class);
    }

    @Test
    void validationErrorIsABug() {
        var result = classify(reason("TransactionConflict"), reason("ValidationError"), reason("None"));

        assertThat(result).isInstanceOf(TransactionCanceledException.class);
    }

    static RuntimeException classify(CancellationReason... reasons) {
        var e = TransactionCanceledException.builder().cancellationReasons(Arrays.asList(reasons)).build();
        return TxOutcomes.classify(e, ITEMS);
    }

    static CancellationReason reason(String code) {
        return CancellationReason.builder().code(code).build();
    }

    static CancellationReason failed(Map<String, AttributeValue> old) {
        return CancellationReason.builder().code("ConditionalCheckFailed").item(old).build();
    }
}
