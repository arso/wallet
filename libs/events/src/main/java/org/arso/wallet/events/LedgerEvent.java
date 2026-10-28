package org.arso.wallet.events;

import java.time.Instant;
import java.util.Objects;

/**
 * One ledger entry on {@code ledger-events}, the full ledger for any downstream reader (audit, AML, reporting). A field
 * stays even when no consumer in this repo reads it; leave out only what a reader can derive by replaying the account's
 * earlier events.
 *
 * @param schemaVersion lets consumers reject a payload shape they do not know
 * @param accountId     the account this entry belongs to (also the Kafka key)
 * @param seq           gap-free position in the account's ledger, for ordering and deduplication
 * @param eventType     what happened, see the ledger event types
 * @param recordedAt    when the entry was written
 * @param balanceAfter  balance snapshot, so a reader joining after retention dropped seq 1 can start from it
 * @param actor         who made the change (user, a service, aml, internal)
 * @param reason        why: reject reason, block reason or the error class of a hold
 * @param paymentId     links a withdrawal's entries together
 * @param amount        money moved, in minor units
 * @param currency      currency of {@code amount}
 * @param externalTxId  provider id of a deposit or of a completed transfer
 * @param payer         who sent a deposit
 * @param beneficiary   who receives a withdrawal
 * @param verifier      the step that rejected a withdrawal; not in any earlier event
 * @param holdRef       the failed Kafka record ({@code topic/partition/offset}) that put the account on hold, for recovery
 * @param releasedTo    status the account returned to after a hold
 */
public record LedgerEvent(
        int schemaVersion,
        String accountId,
        long seq,
        String eventType,
        Instant recordedAt,
        long balanceAfter,
        String actor,
        String reason,
        String paymentId,
        Long amount,
        String currency,
        String externalTxId,
        Payer payer,
        Beneficiary beneficiary,
        String verifier,
        String holdRef,
        String releasedTo) {

    public LedgerEvent {
        Objects.requireNonNull(accountId, "account_id");
        Objects.requireNonNull(eventType, "event_type");
        Objects.requireNonNull(recordedAt, "recorded_at");
        Objects.requireNonNull(actor, "actor");
    }
}
