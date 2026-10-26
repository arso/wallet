package org.arso.wallet.worker;

import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface RecordHandler {

    sealed interface Outcome {

        Outcome DONE = new Done();

        record Done() implements Outcome {}

        /** Not done yet: handle the record again at {@code at}. Holds the commit, not later records. */
        record RecheckAt(Instant at) implements Outcome {}
    }

    /** Business answers return DONE; any exception goes through the failure policy (plan 5.6). */
    Outcome handle(ConsumerRecord<String, byte[]> record) throws Exception;
}
