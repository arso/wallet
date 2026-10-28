package org.arso.wallet.worker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.Set;
import java.util.TreeMap;

final class Partition {

    enum Pause {
        NOT_DUE,
        BACKPRESSURE,
        UNCOMMITTED_CAP,
        CIRCUIT_OPEN
    }

    record Pending(ConsumerRecord<String, byte[]> record, Instant due) {
    }

    final TopicPartition tp;
    final ArrayDeque<Pending> pending = new ArrayDeque<>();
    final Set<Pause> reasons = EnumSet.noneOf(Pause.class);
    private final TreeMap<Long, Long> open = new TreeMap<>();
    private long next = -1;
    private long committed = -1;
    int inFlight;
    boolean paused;

    Partition(TopicPartition tp) {
        this.tp = tp;
    }

    void polled(ConsumerRecord<String, byte[]> record, Instant due, long nowMillis) {
        pending.add(new Pending(record, due));
        open.put(record.offset(), nowMillis);
        next = record.offset() + 1;
    }

    void done(long offset) {
        open.remove(offset);
    }

    long position() {
        return open.isEmpty() ? next : open.firstKey();
    }

    long uncommitted() {
        return next < 0 ? 0 : next - position();
    }

    long oldestOpenSince(long nowMillis) {
        return open.isEmpty() ? 0 : nowMillis - open.firstEntry().getValue();
    }

    boolean commitDue() {
        return position() > committed;
    }

    void committed(long position) {
        committed = position;
    }
}
