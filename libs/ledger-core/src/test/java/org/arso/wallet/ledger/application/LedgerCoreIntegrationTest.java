package org.arso.wallet.ledger.application;

import org.arso.wallet.ledger.FlakyDynamoDb;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoAccountHolds;
import org.arso.wallet.ledger.adapter.dynamodb.DynamoLedgerStore;
import org.arso.wallet.ledger.application.Transactions.RetryPolicy;
import org.arso.wallet.ledger.application.Transactions.Writes;
import org.arso.wallet.ledger.domain.*;
import org.arso.wallet.ledger.domain.LedgerEntry.AccountHeld;
import org.arso.wallet.ledger.domain.LedgerEntry.AccountReleased;
import org.arso.wallet.ledger.domain.LedgerEntry.Deposited;
import org.arso.wallet.ledger.domain.LedgerEntry.Withdrawn;
import org.arso.wallet.ledger.port.AccountHolds.Result;
import org.arso.wallet.ledger.port.StepWrite;
import org.arso.wallet.ledger.port.StoredResponse;
import org.arso.wallet.ledger.port.TxItem.PutAccount;
import org.arso.wallet.ledger.port.TxItem.PutEntry;
import org.arso.wallet.ledger.port.TxItem.PutPayment;
import org.arso.wallet.testing.DynamoDbLocal;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.net.HttpURLConnection.*;
import static org.arso.wallet.ledger.FlakyDynamoDb.Fault.CONFLICT_BEFORE_COMMIT;
import static org.arso.wallet.ledger.FlakyDynamoDb.Fault.TIMEOUT_AFTER_COMMIT;
import static org.arso.wallet.ledger.domain.PaymentStatus.*;
import static org.assertj.core.api.Assertions.assertThat;

class LedgerCoreIntegrationTest {

    static final Counterparty PARTY = new Counterparty("p-1", "John", "****1234", "hmac:1", "PL");
    static final Clock CLOCK = Clock.systemUTC();

    final DynamoDbClient raw = DynamoDbLocal.client();
    final FlakyDynamoDb client = new FlakyDynamoDb(raw);
    final String table = DynamoDbLocal.newTable(raw);
    final DynamoLedgerStore store = new DynamoLedgerStore(client, table, Duration.ofHours(24));
    final DynamoAccountHolds holds = new DynamoAccountHolds(raw, table, CLOCK);
    final Transactions transactions = new Transactions(store, new RetryPolicy(5, Duration.ofMillis(20)));
    final PaymentTransitions payments = new PaymentTransitions(store, transactions, CLOCK);

    @Test
    void ledgerEntriesRoundTripNewestFirst() {
        var id = account(100);
        var payment = pendingWithdraw(id, 30, List.of("aml"));
        holds.holdAccount(id.value(), "NullPointerException", "topic/1/5", Actor.COMPLETION_SERVICE);
        transactions.onAccount(id, a -> change(a.release("fixed", AccountStatus.ACTIVE, false, Actor.INTERNAL, now())));

        var page = store.entries(id, null, 3);
        assertThat(page).extracting(e -> e.meta().seq()).containsExactly(5L, 4L, 3L);
        assertThat(page.get(0)).isInstanceOfSatisfying(AccountReleased.class, r -> assertThat(r.releasedTo())
                .isEqualTo(AccountStatus.ACTIVE));
        assertThat(page.get(1)).isInstanceOfSatisfying(AccountHeld.class, h -> assertThat(h.holdRef())
                .isEqualTo("topic/1/5"));
        assertThat(page.get(2)).isInstanceOfSatisfying(Withdrawn.class, w -> {
            assertThat(w.paymentId()).isEqualTo(payment.id());
            assertThat(w.beneficiary()).isEqualTo(PARTY);
        });
        assertThat(store.entries(id, 3L, 50)).extracting(LedgerEntry::type)
                .containsExactly(LedgerEventType.DEPOSIT, LedgerEventType.ACCOUNT_CREATED);
        assertThat(store.findEntry(id, 2)).get().isInstanceOf(Deposited.class);
        assertThat(store.entries(id, 1L, 50)).isEmpty();
    }

    @Test
    void holdIsStoredOnceAndReleaseClearsIt() {
        var id = account(10);
        transactions.onAccount(id, a -> change(a.block("aml", Actor.AML, now())));

        assertThat(holds.holdAccount(id.value(), "e", "t/0/1", Actor.COMPLETION_SERVICE)).isEqualTo(Result.HELD);
        assertThat(holds.holdAccount(id.value(), "e", "t/0/2", Actor.COMPLETION_SERVICE)).isEqualTo(Result.HELD);
        assertThat(holds.holdAccount(id.value(), "e", "t/0/1", Actor.COMPLETION_SERVICE)).isEqualTo(Result.ALREADY_HELD);

        var held = store.findAccount(id).orElseThrow();
        assertThat(held.status()).isEqualTo(AccountStatus.ON_HOLD);
        assertThat(held.preHoldStatus()).isEqualTo(AccountStatus.BLOCKED);
        assertThat(held.holdRefs()).containsExactlyInAnyOrder("t/0/1", "t/0/2");
        assertThat(held.version()).isEqualTo(5);

        transactions.onAccount(id, a -> change(a.release("ok", AccountStatus.BLOCKED, false, Actor.INTERNAL, now())));
        var released = store.findAccount(id).orElseThrow();
        assertThat(released.preHoldStatus()).isNull();
        assertThat(released.holdRefs()).isEmpty();
    }

    @Test
    void holdFindsTheAccountOfAPaymentOrIsUnattributable() {
        var id = account(100);
        var payment = pendingWithdraw(id, 30, List.of("aml"));

        assertThat(holds.holdPaymentAccount(payment.id().value(), "e", "t/1/9", Actor.TIMEOUT_CHECKER)).isEqualTo(Result.HELD);
        assertThat(holds.holdPaymentAccount(PaymentId.random().value(), "e", "t/1/10", Actor.TIMEOUT_CHECKER))
                .isEqualTo(Result.UNATTRIBUTABLE);
        assertThat(holds.holdPaymentAccount("not-a-ulid", "e", "t/1/11", Actor.TIMEOUT_CHECKER)).isEqualTo(Result.UNATTRIBUTABLE);
        assertThat(holds.holdAccount(AccountId.random().value(), "e", "t/1/12", Actor.TIMEOUT_CHECKER))
                .isEqualTo(Result.UNATTRIBUTABLE);

        var held = store.findAccount(id).orElseThrow();
        assertThat(held.status()).isEqualTo(AccountStatus.ON_HOLD);
        assertThat(held.preHoldStatus()).isEqualTo(AccountStatus.ACTIVE);
        assertThat(store.findEntry(id, held.version())).get().isInstanceOfSatisfying(AccountHeld.class, h -> {
            assertThat(h.holdRef()).isEqualTo("t/1/9");
            assertThat(h.meta().balanceAfter()).isEqualTo(70);
        });
        assertLedgerMatches(held);
    }

    @Test
    void stepIsWrittenOnceAndLateAfterTheFinalState() {
        var id = account(100);
        var payment = pendingWithdraw(id, 30, List.of("aml", "kyc"));

        var recorded = store.recordVerification(payment.id(), "aml", verdict(true));
        var duplicate = store.recordVerification(payment.id(), "aml", verdict(false));
        timeOut(payment.id());
        var late = store.recordVerification(payment.id(), "kyc", verdict(true));

        assertThat(recorded).isInstanceOfSatisfying(StepWrite.Recorded.class, r -> assertThat(r.payment().verifications())
                .containsOnlyKeys("aml"));
        assertThat(duplicate).isInstanceOfSatisfying(StepWrite.Duplicate.class, d -> assertThat(
                d.payment().verifications().get("aml").allowed()).isTrue());
        assertThat(late).isInstanceOfSatisfying(StepWrite.Late.class, l -> assertThat(
                l.payment().verifications().get("kyc").late()).isTrue());
        assertThat(store.recordVerification(payment.id(), "kyc", verdict(true))).isInstanceOf(StepWrite.Duplicate.class);
        assertThat(store.recordVerification(PaymentId.random(), "aml", verdict(true))).isInstanceOf(StepWrite.NotFound.class);
    }

    @Test
    void storedResponseIsWrittenOnceAndExpiredOneIsAbsent() {
        var first = new StoredResponse("h1", HTTP_BAD_REQUEST, "{}");
        var now = now();

        assertThat(store.storeResponse("k1", first, now)).isTrue();
        assertThat(store.storeResponse("k1", new StoredResponse("h2", HTTP_CONFLICT, "{}"), now)).isFalse();
        assertThat(store.findResponse("k1", now)).contains(first);

        var afterTtl = now.plus(Duration.ofHours(25));
        assertThat(store.findResponse("k1", afterTtl)).isEmpty();
        assertThat(store.storeResponse("k1", new StoredResponse("h3", HTTP_CREATED, "{}"), afterTtl)).isTrue();
    }

    @Test
    void concurrentWithdrawalsNeverOverdraw() throws Exception {
        var id = account(100);
        var succeeded = new AtomicInteger();
        var concurrent = new Transactions(store, new RetryPolicy(50, Duration.ofMillis(20)));

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            IntStream.range(0, 20).forEach(_ -> executor.submit(() -> {
                try {
                    concurrent.onAccount(id, a -> change(a.withdraw(usd(10), PaymentId.random(), PARTY, now())));
                    succeeded.incrementAndGet();
                } catch (DomainException e) {
                    assertThat(e.code()).isEqualTo(DomainException.Code.INSUFFICIENT_FUNDS);
                }
            }));
        }

        var account = store.findAccount(id).orElseThrow();
        assertThat(succeeded).hasValue(10);
        assertThat(account.balance()).isEqualTo(usd(0));
        assertLedgerMatches(account);
    }

    @Test
    void retryAfterTimeoutWithSameTokenAppliesOnce() {
        var id = account(0);
        client.failNext(TIMEOUT_AFTER_COMMIT);

        transactions.onAccount(id, a -> change(a.deposit(usd(5), "tx-2", PARTY, now())));

        var account = store.findAccount(id).orElseThrow();
        assertThat(account.balance()).isEqualTo(usd(5));
        assertThat(account.version()).isEqualTo(2);
        assertLedgerMatches(account);
    }

    @Test
    void markExecutingSurvivesTransactionConflict() {
        var id = account(100);
        var payment = pendingWithdraw(id, 10, List.of("aml"), Map.of("aml", verdict(true)));
        client.failNext(CONFLICT_BEFORE_COMMIT);

        var result = payments.markExecuting(payment, Actor.COMPLETION_SERVICE);

        assertThat(result.status()).isEqualTo(EXECUTING);
        assertThat(store.findPayment(payment.id()).orElseThrow().status()).isEqualTo(EXECUTING);
    }

    @Test
    void rejectAndCompleteSurviveTransactionConflict() {
        var id = account(100);
        var rejected = pendingWithdraw(id, 10, List.of());
        var completed = pendingWithdraw(id, 20, List.of());
        payments.markExecuting(completed, Actor.COMPLETION_SERVICE);

        client.failNext(CONFLICT_BEFORE_COMMIT);
        timeOut(rejected.id());
        client.failNext(CONFLICT_BEFORE_COMMIT);
        payments.complete(completed.id(), "ext-1", Actor.TRANSFER_EXECUTOR);

        assertThat(store.findPayment(rejected.id()).orElseThrow().status()).isEqualTo(REJECTED);
        assertThat(store.findPayment(completed.id()).orElseThrow().externalTxId()).isEqualTo("ext-1");
        assertThat(store.findPayment(completed.id()).orElseThrow().status()).isEqualTo(COMPLETED);
        var account = store.findAccount(id).orElseThrow();
        assertThat(account.balance()).isEqualTo(usd(80));
        assertLedgerMatches(account);
    }

    @Test
    void rejectIsAppliedOnce() {
        var id = account(100);
        var payment = pendingWithdraw(id, 10, List.of());

        timeOut(payment.id());
        payments.reject(payment, RejectReason.VERIFICATION_TIMEOUT, null, Actor.TIMEOUT_CHECKER);

        assertThat(store.findAccount(id).orElseThrow().balance()).isEqualTo(usd(100));
    }

    @Test
    void storedRejectWinsOverStaleAllowedCopy() {
        var id = account(100);
        var stale = allowed(pendingWithdraw(id, 10, List.of("aml", "kyc"), Map.of("aml", verdict(false))));

        var result = payments.markExecuting(stale, Actor.COMPLETION_SERVICE);

        assertThat(result.status()).isEqualTo(REJECTED);
        assertThat(result.rejectReason()).isEqualTo(RejectReason.VERIFICATION_REJECTED);
        assertThat(store.findAccount(id).orElseThrow().balance()).isEqualTo(usd(100));
    }

    @Test
    void blockedAccountRejectsAtMarkExecuting() {
        var id = account(100);
        var payment = pendingWithdraw(id, 10, List.of("aml"), Map.of("aml", verdict(true)));
        transactions.onAccount(id, a -> change(a.block("aml", Actor.AML, now())));

        var result = payments.markExecuting(payment, Actor.COMPLETION_SERVICE);

        assertThat(result.rejectReason()).isEqualTo(RejectReason.ACCOUNT_BLOCKED);
        assertThat(store.findAccount(id).orElseThrow().balance()).isEqualTo(usd(100));
    }

    @Test
    void everyDecidingReadIsConsistent() {
        var id = account(10);
        var payment = pendingWithdraw(id, 5, List.of());
        timeOut(payment.id());
        store.findResponse("missing", now());

        assertThat(client.gets).isNotEmpty().allSatisfy(get -> assertThat(get.consistentRead()).isTrue());
    }

    AccountId account(long balance) {
        var opened = Account.open(AccountId.random(), Currency.USD, now());
        transactions.commit(List.of(new PutAccount(opened.account()), new PutEntry(opened.entry())));
        var id = opened.account().id();
        if (balance > 0) {
            transactions.onAccount(id, a -> change(a.deposit(usd(balance), "tx-" + id, PARTY, now())));
        }
        return id;
    }

    Payment pendingWithdraw(AccountId id, long amount, List<String> steps) {
        return pendingWithdraw(id, amount, steps, Map.of());
    }

    Payment pendingWithdraw(AccountId id, long amount, List<String> steps, Map<String, Verification> verifications) {
        return transactions.onAccount(id, a -> {
            var paymentId = PaymentId.random();
            var change = a.withdraw(usd(amount), paymentId, PARTY, now());
            var payment = Payment.create(paymentId, id, usd(amount), "b-1", steps, now());
            for (var v : verifications.entrySet()) {
                payment = payment.recordVerification(v.getKey(), v.getValue());
            }
            return new Writes<>(
                    List.of(new PutAccount(change.account()), new PutEntry(change.entry()), new PutPayment(payment)),
                    payment);
        });
    }

    void timeOut(PaymentId id) {
        payments.reject(store.findPayment(id).orElseThrow(), RejectReason.VERIFICATION_TIMEOUT, null, Actor.TIMEOUT_CHECKER);
    }

    static Payment allowed(Payment p) {
        var all = p.requiredSteps().stream().collect(Collectors.toMap(s -> s, _ -> verdict(true)));
        return new Payment(
                p.id(), p.accountId(), p.amount(), p.beneficiaryId(), p.status(), p.receivedAt(), null, null, null, null,
                p.requiredSteps(), all);
    }

    void assertLedgerMatches(Account account) {
        var entries = store.entries(account.id(), null, 200).reversed();
        assertThat(entries).extracting(e -> e.meta().seq())
                .containsExactlyElementsOf(IntStream.rangeClosed(1, (int) account.version()).mapToObj(Long::valueOf).toList());
        var replayed = entries.stream().mapToLong(LedgerCoreIntegrationTest::delta).sum();
        assertThat(replayed).isEqualTo(account.balance().amountMinor());
        assertThat(entries.getLast().meta().balanceAfter()).isEqualTo(account.balance().amountMinor());
    }

    static long delta(LedgerEntry entry) {
        return switch (entry) {
            case LedgerEntry.Deposited d -> d.amount().amountMinor();
            case LedgerEntry.Withdrawn w -> -w.amount().amountMinor();
            case LedgerEntry.UserCancelledWithdraw c -> c.amount().amountMinor();
            case LedgerEntry.WithdrawRejected r -> r.amount().amountMinor();
            default -> 0;
        };
    }

    static Verification verdict(boolean allowed) {
        return new Verification(allowed, null, now(), now(), false);
    }

    static Writes<Account> change(Account.Change change) {
        return new Writes<>(List.of(new PutAccount(change.account()), new PutEntry(change.entry())), change.account());
    }

    static Money usd(long amount) {
        return new Money(amount, Currency.USD);
    }

    static Instant now() {
        return CLOCK.instant().truncatedTo(ChronoUnit.MILLIS);
    }
}
