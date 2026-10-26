package org.arso.wallet.bridge.adapter.stream;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.arso.wallet.bridge.application.Bridge;
import org.arso.wallet.ledger.adapter.dynamodb.LedgerItems;
import org.arso.wallet.ledger.domain.LedgerEntry;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

/** Local stand-in for the Lambda event source mapping, on DynamoDB Local. Not for production. */
public final class StreamRunner implements Runnable {

    public record Settings(ShardIteratorType startAt, Duration idleWait, Duration retryWait) {}

    private static final System.Logger LOG = System.getLogger(StreamRunner.class.getName());

    private final DynamoDbStreamsClient streams;
    private final String streamArn;
    private final Bridge bridge;
    private final Settings settings;
    private final Map<String, String> iterators = new LinkedHashMap<>();
    private final Set<String> finished = new HashSet<>();
    private volatile boolean running = true;

    public StreamRunner(DynamoDbStreamsClient streams, String streamArn, Bridge bridge, Settings settings) {
        this.streams = streams;
        this.streamArn = streamArn;
        this.bridge = bridge;
        this.settings = settings;
    }

    public static String streamArn(DynamoDbClient client, String table) {
        return client.describeTable(b -> b.tableName(table)).table().latestStreamArn();
    }

    @Override
    public void run() {
        LOG.log(INFO, "polling stream " + streamArn);
        while (running) {
            try {
                if (!pollOnce()) {
                    Thread.sleep(settings.idleWait());
                }
            } catch (RuntimeException e) {
                LOG.log(WARNING, "batch failed, resending", e);
                sleep(settings.retryWait());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public void stop() {
        running = false;
    }

    public boolean pollOnce() {
        discoverShards();
        var read = false;
        RuntimeException failure = null;
        for (var shardId : List.copyOf(iterators.keySet())) {
            try {
                read |= pollShard(shardId);
            } catch (RuntimeException e) {
                failure = e;
            }
        }
        if (failure != null) {
            throw failure;
        }
        return read;
    }

    private boolean pollShard(String shardId) {
        var response = streams.getRecords(b -> b.shardIterator(iterators.get(shardId)));
        // the iterator moves only after the batch is published; a failure re-reads the same batch
        bridge.publish(ledgerEntries(response.records()));
        if (response.nextShardIterator() == null) {
            iterators.remove(shardId);
            finished.add(shardId);
        } else {
            iterators.put(shardId, response.nextShardIterator());
        }
        return !response.records().isEmpty();
    }

    private void discoverShards() {
        var shards = streams.describeStream(b -> b.streamArn(streamArn)).streamDescription().shards();
        var known = new HashSet<String>();
        shards.forEach(s -> known.add(s.shardId()));
        for (var shard : shards) {
            var id = shard.shardId();
            var parent = shard.parentShardId();
            if (iterators.containsKey(id) || finished.contains(id)) {
                continue;
            }
            if (parent == null || finished.contains(parent) || !known.contains(parent)) {
                var type = finished.isEmpty() ? settings.startAt() : ShardIteratorType.TRIM_HORIZON;
                iterators.put(id, streams.getShardIterator(b -> b.streamArn(streamArn).shardId(id).shardIteratorType(type))
                        .shardIterator());
            }
        }
    }

    private static List<LedgerEntry> ledgerEntries(List<Record> records) {
        return records.stream()
                .filter(r -> r.eventName() == OperationType.INSERT && LedgerItems.isLedgerItem(r.dynamodb().keys()))
                .map(r -> LedgerItems.entry(r.dynamodb().newImage()))
                .toList();
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
