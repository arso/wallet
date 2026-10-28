package org.arso.wallet.bridge;

import org.arso.wallet.bridge.port.EventPublisher;
import org.arso.wallet.events.LedgerEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

public final class RecordingPublisher implements EventPublisher {

    public final List<List<Message>> calls = new CopyOnWriteArrayList<>();
    public final List<Message> sent = new CopyOnWriteArrayList<>();
    private final List<Predicate<Message>> failOnce = new ArrayList<>();

    public synchronized RecordingPublisher failOnce(Predicate<Message> match) {
        failOnce.add(match);
        return this;
    }

    @Override
    public synchronized void publishAll(List<Message> messages) {
        calls.add(List.copyOf(messages));
        Message failed = null;
        for (var message : messages) {
            var rule = failOnce.stream().filter(r -> r.test(message)).findFirst();
            if (rule.isPresent()) {
                failOnce.remove(rule.get());
                failed = message;
            } else {
                sent.add(message);
            }
        }
        if (failed != null) {
            throw new PublishFailedException("not acked: " + failed.key(), new RuntimeException("broker down"));
        }
    }

    public static Predicate<Message> ledgerSeq(long seq) {
        return m -> m.payload() instanceof LedgerEvent e && e.seq() == seq;
    }

    public static List<Long> ledgerSeqs(List<Message> messages) {
        return messages.stream()
                .filter(m -> m.payload() instanceof LedgerEvent)
                .map(m -> ((LedgerEvent) m.payload()).seq())
                .toList();
    }
}
