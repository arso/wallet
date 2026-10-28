package org.arso.wallet.bridge.adapter.stream;

import org.arso.wallet.bridge.RecordingPublisher;
import org.arso.wallet.bridge.application.Bridge;
import org.arso.wallet.events.Events;
import org.arso.wallet.events.WithdrawRequest;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.testing.DynamoDbLocal;
import org.arso.wallet.testing.TestLedger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.arso.wallet.bridge.RecordingPublisher.ledgerSeq;
import static org.arso.wallet.bridge.RecordingPublisher.ledgerSeqs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamRunnerTest {

    final TestLedger ledger = new TestLedger();
    final RecordingPublisher publisher = new RecordingPublisher();
    final StreamRunner runner = new StreamRunner(
            DynamoDbLocal.streamsClient(),
            StreamRunner.streamArn(ledger.client, ledger.table),
            new Bridge(publisher),
            new StreamRunner.Settings(ShardIteratorType.TRIM_HORIZON, Duration.ofMillis(20), Duration.ofMillis(20)));

    @Test
    void publishesOnlyLedgerInsertsInOrder() {
        var id = ledger.open();
        ledger.deposit(id, 100);
        var payment = ledger.withdraw(id, 40);

        pollUntil(() -> publisher.sent.size() == 4);

        assertThat(ledgerSeqs(publisher.sent)).containsExactly(1L, 2L, 3L);
        assertThat(publisher.sent).filteredOn(m -> m.topic().equals(Events.WITHDRAW_REQUESTS)).singleElement()
                .satisfies(m -> assertThat(((WithdrawRequest) m.payload()).paymentId()).isEqualTo(payment.id().value()));
    }

    @Test
    void failedPublishResendsTheSameBatchAndSkipsNothing() {
        var id = ledger.open();
        ledger.deposit(id, 100);
        ledger.deposit(id, 200);
        publisher.failOnce(ledgerSeq(2));

        pollUntil(() -> ledgerSeqs(publisher.sent).containsAll(List.of(2L, 3L)));

        assertThat(ledgerSeqs(publisher.sent)).containsOnly(1L, 2L, 3L);
        var withSeq2 = publisher.calls.stream().filter(c -> ledgerSeqs(c).contains(2L)).toList();
        assertThat(withSeq2).hasSize(2);
        assertThat(withSeq2.get(1)).isEqualTo(withSeq2.get(0));
    }

    @Test
    void unmappableEntryStallsTheShardAndNothingAfterItIsPublished() {
        var id = ledger.open();
        putBrokenEntry(id, 900);
        ledger.deposit(id, 100);

        var deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (true) {
            assertThat(System.nanoTime()).as("timed out").isLessThan(deadline);
            try {
                runner.pollOnce();
            } catch (RuntimeException e) {
                assertThat(e).isInstanceOf(NullPointerException.class);
                break;
            }
        }
        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(runner::pollOnce).isInstanceOf(NullPointerException.class);
        }

        assertThat(ledgerSeqs(publisher.sent)).doesNotContain(2L, 900L);
    }

    void putBrokenEntry(AccountId id, long seq) {
        ledger.client.putItem(b -> b.tableName(ledger.table).item(Map.of(
                "PK", AttributeValue.fromS(id.value()),
                "SK", AttributeValue.fromS("LEDGER#%012d".formatted(seq)),
                "account_id", AttributeValue.fromS(id.value()),
                "seq", AttributeValue.fromN(Long.toString(seq)),
                "event_type", AttributeValue.fromS("deposit"))));
    }

    void pollUntil(BooleanSupplier done) {
        var deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!done.getAsBoolean()) {
            assertThat(System.nanoTime()).as("timed out").isLessThan(deadline);
            try {
                runner.pollOnce();
            } catch (RuntimeException _) {
                // resent on the next poll
            }
        }
    }
}
