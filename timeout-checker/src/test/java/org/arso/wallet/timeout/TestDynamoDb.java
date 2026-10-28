package org.arso.wallet.timeout;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;

/**
 * DynamoDB Local with stale reads, throttled transactions and failing payment reads, which it cannot produce itself.
 */
final class TestDynamoDb implements DynamoDbClient {

    private final DynamoDbClient delegate;
    private final String table;
    private final Map<String, GetItemResponse> frozen = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> throttled = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> broken = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> reads = new ConcurrentHashMap<>();

    TestDynamoDb(DynamoDbClient delegate, String table) {
        this.delegate = delegate;
        this.table = table;
    }

    /**
     * Eventually consistent reads of this payment keep returning it as it is now.
     */
    void freezePayment(String paymentId) {
        var key = Map.of("PK", AttributeValue.fromS("PAYMENT#" + paymentId), "SK", AttributeValue.fromS("PAYMENT#"));
        frozen.put(key.get("PK").s(), delegate.getItem(b -> b.tableName(table).key(key)));
    }

    /**
     * The next transactions that mention this text fail with throttling.
     */
    void throttle(String text, int times) {
        throttled.put(text, new AtomicInteger(times));
    }

    /**
     * The next full reads of this payment fail like a mapper bug; the hold flow's projected read still works.
     */
    void breakPayment(String paymentId, int times) {
        broken.put("PAYMENT#" + paymentId, new AtomicInteger(times));
    }

    /**
     * Full reads of this payment so far.
     */
    int reads(String paymentId) {
        var count = reads.get("PAYMENT#" + paymentId);
        return count == null ? 0 : count.get();
    }

    @Override
    public GetItemResponse getItem(GetItemRequest request) {
        var pk = request.key().get("PK").s();
        if (request.projectionExpression() == null) {
            reads.computeIfAbsent(pk, _ -> new AtomicInteger()).incrementAndGet();
            var left = broken.get(pk);
            if (left != null && left.getAndDecrement() > 0) {
                throw new IllegalStateException("simulated mapper bug");
            }
        }
        var stale = frozen.get(pk);
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
