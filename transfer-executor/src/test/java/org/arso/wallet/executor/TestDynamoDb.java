package org.arso.wallet.executor;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

/** DynamoDB Local with stale eventually consistent reads and throttled transactions, which it cannot produce itself. */
final class TestDynamoDb implements DynamoDbClient {

    private final DynamoDbClient delegate;
    private final String table;
    private final Map<String, GetItemResponse> frozen = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> throttled = new ConcurrentHashMap<>();

    TestDynamoDb(DynamoDbClient delegate, String table) {
        this.delegate = delegate;
        this.table = table;
    }

    /** Eventually consistent reads of this payment keep returning it as it is now. */
    void freezePayment(String paymentId) {
        var key = Map.of("PK", AttributeValue.fromS("PAYMENT#" + paymentId), "SK", AttributeValue.fromS("PAYMENT#"));
        frozen.put(key.get("PK").s(), delegate.getItem(b -> b.tableName(table).key(key)));
    }

    /** The next transactions that mention this text fail with throttling. */
    void throttle(String text, int times) {
        throttled.put(text, new AtomicInteger(times));
    }

    @Override
    public GetItemResponse getItem(GetItemRequest request) {
        var stale = frozen.get(request.key().get("PK").s());
        return stale != null && !Boolean.TRUE.equals(request.consistentRead()) ? stale : delegate.getItem(request);
    }

    @Override
    public TransactWriteItemsResponse transactWriteItems(TransactWriteItemsRequest request) {
        var text = request.toString();
        throttled.forEach((key, left) -> {
            if (text.contains(key) && left.getAndDecrement() > 0) {
                throw DynamoDbException.builder()
                        .statusCode(HTTP_BAD_REQUEST)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("ThrottlingException").build())
                        .build();
            }
        });
        return delegate.transactWriteItems(request);
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
