package org.arso.wallet.worker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionTest {

    final Partition partition = new Partition(new TopicPartition("t", 0));

    @Test
    void failedCommitIsDueAgainWithoutNewProgress() {
        doneUpTo(3);
        partition.committed(partition.position());
        assertThat(partition.commitDue()).isFalse();

        partition.commitFailed(3);

        assertThat(partition.commitDue()).isTrue();
        assertThat(partition.position()).isEqualTo(3);
    }

    @Test
    void failureOfAnOlderCommitKeepsTheNewerOne() {
        doneUpTo(2);
        partition.committed(partition.position());
        doneUpTo(4);
        partition.committed(partition.position());

        partition.commitFailed(2);

        assertThat(partition.commitDue()).isFalse();
    }

    void doneUpTo(long next) {
        for (long offset = Math.max(0, partition.position()); offset < next; offset++) {
            partition.polled(new ConsumerRecord<>("t", 0, offset, "k", new byte[0]), null, 0);
            partition.done(offset);
        }
    }
}
