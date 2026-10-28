package org.arso.wallet.ledger;

import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Real DynamoDB client with scripted transaction failures, for cases DynamoDB Local cannot produce.
 */
public final class FlakyDynamoDb implements DynamoDbClient {

    public enum Fault {
        CONFLICT_BEFORE_COMMIT,
        TIMEOUT_AFTER_COMMIT
    }

    private final DynamoDbClient delegate;
    private final Deque<Fault> faults = new ArrayDeque<>();
    public final List<GetItemRequest> gets = new CopyOnWriteArrayList<>();
    public final List<TransactWriteItemsRequest> transactions = new CopyOnWriteArrayList<>();

    public FlakyDynamoDb(DynamoDbClient delegate) {
        this.delegate = delegate;
    }

    public synchronized FlakyDynamoDb failNext(Fault... next) {
        faults.addAll(List.of(next));
        return this;
    }

    @Override
    public TransactWriteItemsResponse transactWriteItems(TransactWriteItemsRequest request) {
        transactions.add(request);
        Fault fault;
        synchronized (this) {
            fault = faults.poll();
        }
        if (fault == Fault.CONFLICT_BEFORE_COMMIT) {
            var reasons = request.transactItems().stream()
                    .map(_ -> CancellationReason.builder().code("TransactionConflict").build())
                    .toList();
            throw TransactionCanceledException.builder().cancellationReasons(reasons).build();
        }
        var response = delegate.transactWriteItems(request);
        if (fault == Fault.TIMEOUT_AFTER_COMMIT) {
            throw ApiCallTimeoutException.create(1000);
        }
        return response;
    }

    @Override
    public GetItemResponse getItem(GetItemRequest request) {
        gets.add(request);
        return delegate.getItem(request);
    }

    @Override
    public PutItemResponse putItem(PutItemRequest request) {
        return delegate.putItem(request);
    }

    @Override
    public UpdateItemResponse updateItem(UpdateItemRequest request) {
        return delegate.updateItem(request);
    }

    @Override
    public QueryResponse query(QueryRequest request) {
        return delegate.query(request);
    }

    @Override
    public String serviceName() {
        return delegate.serviceName();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
