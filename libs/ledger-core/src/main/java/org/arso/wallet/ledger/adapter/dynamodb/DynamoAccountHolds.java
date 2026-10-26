package org.arso.wallet.ledger.adapter.dynamodb;

import static org.arso.wallet.ledger.adapter.dynamodb.Items.num;
import static org.arso.wallet.ledger.adapter.dynamodb.Items.str;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.arso.wallet.ledger.application.LockRetriesExhaustedException;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.AccountStatus;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.domain.LedgerEntry.AccountHeld;
import org.arso.wallet.ledger.domain.LedgerEntry.Meta;
import org.arso.wallet.ledger.domain.Timestamps;
import org.arso.wallet.ledger.domain.Ulid;
import org.arso.wallet.ledger.port.AccountHolds;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;

public final class DynamoAccountHolds implements AccountHolds {

    private static final int MAX_ATTEMPTS = 5;

    private final DynamoDbClient client;
    private final String table;
    private final Clock clock;

    public DynamoAccountHolds(DynamoDbClient client, String table, Clock clock) {
        this.client = client;
        this.table = table;
        this.clock = clock;
    }

    @Override
    public Result holdPaymentAccount(String paymentId, String reason, String holdRef, Actor actor) {
        if (!Ulid.isValid(paymentId)) {
            return Result.UNATTRIBUTABLE;
        }
        return read(Items.key(Items.PAYMENT + paymentId, Items.PAYMENT), "account_id")
                .map(p -> holdAccount(Items.s(p, "account_id"), reason, holdRef, actor))
                .orElse(Result.UNATTRIBUTABLE);
    }

    @Override
    public Result holdAccount(String accountId, String reason, String holdRef, Actor actor) {
        if (!Ulid.isValid(accountId)) {
            return Result.UNATTRIBUTABLE;
        }
        var id = new AccountId(accountId);
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            var account = read(Items.accountKey(id), "#s, balance, version, hold_refs");
            if (account.isEmpty()) {
                return Result.UNATTRIBUTABLE;
            }
            var a = account.get();
            if (a.containsKey("hold_refs") && a.get("hold_refs").ss().contains(holdRef)) {
                return Result.ALREADY_HELD;
            }
            try {
                write(id, a, reason, holdRef, actor);
                return Result.HELD;
            } catch (TransactionCanceledException e) {
                if (e.cancellationReasons().stream().noneMatch(r -> "ConditionalCheckFailed".equals(r.code()))) {
                    throw e;
                }
            }
        }
        throw new LockRetriesExhaustedException(MAX_ATTEMPTS);
    }

    private void write(AccountId id, Map<String, AttributeValue> account, String reason, String holdRef, Actor actor) {
        var now = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        var version = Items.n(account, "version");
        var prior = AccountStatus.valueOf(Items.s(account, "status"));
        var entry = new AccountHeld(new Meta(id, version + 1, now, Items.n(account, "balance"), actor, reason), holdRef, prior);
        var update = Update.builder()
                .tableName(table)
                .key(Items.accountKey(id))
                .updateExpression("SET #s = :held, version = :next, updated_at = :now,"
                        + " pre_hold_status = if_not_exists(pre_hold_status, :prior) ADD hold_refs :refs")
                .conditionExpression("version = :v AND (attribute_not_exists(hold_refs) OR NOT contains(hold_refs, :ref))")
                .expressionAttributeNames(Map.of("#s", "status"))
                .expressionAttributeValues(Map.of(
                        ":held", str(AccountStatus.ON_HOLD.name()),
                        ":next", num(version + 1),
                        ":v", num(version),
                        ":now", str(Timestamps.format(now)),
                        ":prior", str(prior.name()),
                        ":refs", AttributeValue.fromSs(List.of(holdRef)),
                        ":ref", str(holdRef)))
                .build();
        var put = Put.builder().tableName(table).item(Items.entry(entry)).conditionExpression("attribute_not_exists(PK)").build();
        client.transactWriteItems(b -> b.transactItems(
                TransactWriteItem.builder().update(update).build(), TransactWriteItem.builder().put(put).build()));
    }

    private Optional<Map<String, AttributeValue>> read(Map<String, AttributeValue> key, String projection) {
        var request = GetItemRequest.builder().tableName(table).key(key).consistentRead(true).projectionExpression(projection);
        if (projection.contains("#s")) {
            request.expressionAttributeNames(Map.of("#s", "status"));
        }
        var response = client.getItem(request.build());
        return response.hasItem() && !response.item().isEmpty() ? Optional.of(response.item()) : Optional.empty();
    }
}
