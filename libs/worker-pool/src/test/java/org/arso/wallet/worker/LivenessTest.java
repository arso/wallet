package org.arso.wallet.worker;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.arso.wallet.worker.RecordHandler.Outcome;
import org.arso.wallet.worker.WorkerSettings.HoldTarget;
import org.arso.wallet.worker.WorkerSettings.Mode;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class LivenessTest extends PoolTest {

    @Test
    void rebalanceKeepsBothPollThreadsAlive() throws Exception {
        createTopic(2);
        var a = start(settings(Mode.UNORDERED, HoldTarget.PAYMENT_ID), _ -> Outcome.DONE);
        send(0, "k", "v");
        await(() -> committed(0) == 1);

        var b = start(settings(Mode.UNORDERED, HoldTarget.PAYMENT_ID), _ -> Outcome.DONE);
        await(() -> members() == 2);

        assertThat(a.isAlive()).isTrue();
        assertThat(b.isAlive()).isTrue();
    }

    @Test
    void fencedPoolReportsDeadPollThreadWhileTheReplacementConsumes() throws Exception {
        createTopic(1);
        var seen = ConcurrentHashMap.<String>newKeySet();
        Map<String, Object> sameInstance = Map.of(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance-1");
        var a = start(settings(Mode.UNORDERED, HoldTarget.PAYMENT_ID), r -> {
            seen.add("a:" + new String(r.value()));
            return Outcome.DONE;
        }, sameInstance);
        send(0, "k", "first");
        await(() -> seen.contains("a:first"));

        var b = start(settings(Mode.UNORDERED, HoldTarget.PAYMENT_ID), r -> {
            seen.add("b:" + new String(r.value()));
            return Outcome.DONE;
        }, sameInstance);
        await(() -> !a.isAlive());
        send(0, "k", "second");
        await(() -> seen.contains("b:second"));

        assertThat(b.isAlive()).isTrue();
    }

    private int members() {
        try {
            return memberIds().size();
        } catch (Exception e) {
            return 0;
        }
    }
}
