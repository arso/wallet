package org.arso.wallet.ledger.adapter.dynamodb;

import org.arso.wallet.ledger.domain.*;
import org.arso.wallet.ledger.port.*;
import org.arso.wallet.ledger.port.TxItem.*;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusFamily;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.arso.wallet.ledger.adapter.dynamodb.Items.num;
import static org.arso.wallet.ledger.adapter.dynamodb.Items.str;

public final class DynamoLedgerStore implements LedgerStore {

    private static final String NOT_EXISTS = "attribute_not_exists(PK)";
    private static final ReturnValuesOnConditionCheckFailure ALL_OLD = ReturnValuesOnConditionCheckFailure.ALL_OLD;

    private final DynamoDbClient client;
    private final String table;
    private final Duration idempotencyTtl;

    public DynamoLedgerStore(DynamoDbClient client, String table, Duration idempotencyTtl) {
        this.client = client;
        this.table = table;
        this.idempotencyTtl = idempotencyTtl;
    }

    @Override
    public Optional<Account> findAccount(AccountId id) {
        return get(Items.accountKey(id)).map(Items::account);
    }

    @Override
    public Optional<Payment> findPayment(PaymentId id) {
        return get(Items.paymentKey(id)).map(Items::payment);
    }

    @Override
    public Optional<LedgerEntry> findEntry(AccountId id, long seq) {
        return get(Items.ledgerKey(id, seq)).map(Items::entry);
    }

    @Override
    public StepWrite recordVerification(PaymentId id, String step, Verification verification) {
        try {
            return new StepWrite.Recorded(setStep(id, step, verification, true));
        } catch (ConditionalCheckFailedException e) {
            if (!e.hasItem() || e.item().isEmpty()) {
                return new StepWrite.NotFound();
            }
            var current = Items.payment(e.item());
            if (current.verifications().containsKey(step)) {
                return new StepWrite.Duplicate(current);
            }
            try {
                return new StepWrite.Late(setStep(id, step, verification.markLate(), false));
            } catch (ConditionalCheckFailedException again) {
                return new StepWrite.Duplicate(again.hasItem() ? Items.payment(again.item()) : current);
            }
        }
    }

    private Payment setStep(PaymentId id, String step, Verification verification, boolean open) {
        var condition = "attribute_exists(PK) AND attribute_not_exists(#vf.#st)" + (open ? " AND #s IN (:pending, :executing)" : "");
        var names = new HashMap<>(Map.of("#vf", "verifications", "#st", step));
        var values = new HashMap<>(Map.of(":v", AttributeValue.fromM(Items.verification(verification))));
        if (open) {
            names.put("#s", "status");
            values.put(":pending", str(PaymentStatus.PENDING.name()));
            values.put(":executing", str(PaymentStatus.EXECUTING.name()));
        }
        return Items.payment(client.updateItem(b -> b.tableName(table)
                        .key(Items.paymentKey(id))
                        .updateExpression("SET #vf.#st = :v")
                        .conditionExpression(condition)
                        .expressionAttributeNames(names)
                        .expressionAttributeValues(values)
                        .returnValues(ReturnValue.ALL_NEW)
                        .returnValuesOnConditionCheckFailure(ALL_OLD))
                .attributes());
    }

    @Override
    public List<LedgerEntry> entries(AccountId id, Long beforeSeq, int limit) {
        if (beforeSeq != null && beforeSeq <= 1) {
            return List.of();
        }
        var values = new HashMap<String, AttributeValue>();
        values.put(":pk", str(id.value()));
        String condition;
        if (beforeSeq == null) {
            condition = "PK = :pk AND begins_with(SK, :prefix)";
            values.put(":prefix", str(Items.LEDGER));
        } else {
            condition = "PK = :pk AND SK BETWEEN :from AND :to";
            values.put(":from", str(Items.ledgerSk(1)));
            values.put(":to", str(Items.ledgerSk(beforeSeq - 1)));
        }
        return client
                .query(b -> b.tableName(table)
                        .keyConditionExpression(condition)
                        .expressionAttributeValues(values)
                        .scanIndexForward(false)
                        .limit(limit))
                .items()
                .stream()
                .map(Items::entry)
                .toList();
    }

    @Override
    public Optional<StoredResponse> findResponse(String key, Instant now) {
        return get(Items.responseKey(key))
                .filter(m -> Items.n(m, "ttl") >= now.getEpochSecond())
                .map(Items::response);
    }

    @Override
    public boolean storeResponse(String key, StoredResponse response, Instant now) {
        try {
            client.putItem(b -> b.tableName(table)
                    .item(Items.response(key, response, now, idempotencyTtl))
                    .conditionExpression(NOT_EXISTS + " OR #t < :now")
                    .expressionAttributeNames(Map.of("#t", "ttl"))
                    .expressionAttributeValues(Map.of(":now", num(now.getEpochSecond()))));
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public void commit(List<TxItem> items, String token) {
        var writes = items.stream().map(this::write).toList();
        try {
            client.transactWriteItems(b -> b.transactItems(writes).clientRequestToken(token));
        } catch (TransactionCanceledException e) {
            throw TxOutcomes.classify(e, items);
        } catch (TransactionInProgressException | RequestLimitExceededException | SdkClientException e) {
            throw new TransientStoreException(e);
        } catch (DynamoDbException e) {
            if (e.isThrottlingException() || HttpStatusFamily.of(e.statusCode()) == HttpStatusFamily.SERVER_ERROR) {
                throw new TransientStoreException(e);
            }
            throw e;
        }
    }

    private TransactWriteItem write(TxItem item) {
        return switch (item) {
            case PutAccount(var account) -> account.version() == 1
                    ? put(Items.account(account), NOT_EXISTS, Map.of(), Map.of())
                    : put(Items.account(account), "#v = :v", Map.of("#v", "version"), Map.of(":v", num(account.version() - 1)));
            case PutEntry(var entry) -> put(Items.entry(entry), NOT_EXISTS, Map.of(), Map.of());
            case PutPayment(var payment) -> put(Items.payment(payment), NOT_EXISTS, Map.of(), Map.of());
            case UpdatePayment update -> TransactWriteItem.builder().update(update(update)).build();
            case CheckAccountActive(var id) -> TransactWriteItem.builder()
                    .conditionCheck(ConditionCheck.builder()
                            .tableName(table)
                            .key(Items.accountKey(id))
                            .conditionExpression("#s = :active")
                            .expressionAttributeNames(Map.of("#s", "status"))
                            .expressionAttributeValues(Map.of(":active", str(AccountStatus.ACTIVE.name())))
                            .returnValuesOnConditionCheckFailure(ALL_OLD)
                            .build())
                    .build();
            case PutResponse(var key, var response, var now) -> put(
                    Items.response(key, response, now, idempotencyTtl),
                    NOT_EXISTS + " OR #t < :now",
                    Map.of("#t", "ttl"),
                    Map.of(":now", num(now.getEpochSecond())));
            case PutDeposit(var deposit, var now) -> put(Items.deposit(deposit, now), NOT_EXISTS, Map.of(), Map.of());
        };
    }

    private Update update(UpdatePayment update) {
        var p = update.payment();
        var names = new HashMap<String, String>(Map.of("#s", "status"));
        var values = new HashMap<String, AttributeValue>(Map.of(
                ":s", str(p.status().name()),
                ":expected", str(update.expected().name())));
        var set = new StringBuilder("SET #s = :s");
        setIfPresent(set, values, "executing_at", Timestamps.format(p.executingAt()));
        setIfPresent(set, values, "finalized_at", Timestamps.format(p.finalizedAt()));
        setIfPresent(set, values, "reject_reason", p.rejectReason() == null ? null : p.rejectReason().name());
        setIfPresent(set, values, "external_tx_id", p.externalTxId());
        var condition = new StringBuilder("#s = :expected");
        if (update.requireAllowedSteps() && !p.requiredSteps().isEmpty()) {
            names.put("#vf", "verifications");
            values.put(":true", AttributeValue.fromBool(true));
            for (int i = 0; i < p.requiredSteps().size(); i++) {
                names.put("#st" + i, p.requiredSteps().get(i));
                condition.append(" AND #vf.#st").append(i).append(".allowed = :true");
            }
        }
        return Update.builder()
                .tableName(table)
                .key(Items.paymentKey(p.id()))
                .updateExpression(set.toString())
                .conditionExpression(condition.toString())
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .returnValuesOnConditionCheckFailure(ALL_OLD)
                .build();
    }

    private static void setIfPresent(StringBuilder set, Map<String, AttributeValue> values, String field, String value) {
        if (value != null) {
            set.append(", ").append(field).append(" = :").append(field);
            values.put(":" + field, str(value));
        }
    }

    private TransactWriteItem put(
            Map<String, AttributeValue> item, String condition, Map<String, String> names, Map<String, AttributeValue> values) {
        var put = Put.builder()
                .tableName(table)
                .item(item)
                .conditionExpression(condition)
                .returnValuesOnConditionCheckFailure(ALL_OLD);
        if (!names.isEmpty()) {
            put.expressionAttributeNames(names);
        }
        if (!values.isEmpty()) {
            put.expressionAttributeValues(values);
        }
        return TransactWriteItem.builder().put(put.build()).build();
    }

    private Optional<Map<String, AttributeValue>> get(Map<String, AttributeValue> key) {
        var response = client.getItem(b -> b.tableName(table).key(key).consistentRead(true));
        return response.hasItem() && !response.item().isEmpty() ? Optional.of(response.item()) : Optional.empty();
    }
}
