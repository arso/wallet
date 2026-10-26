package org.arso.wallet.api;

import static org.arso.wallet.api.AccountFlowsTest.assertError;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.util.UUID;
import org.arso.wallet.ledger.application.PaymentTransitions;
import org.arso.wallet.ledger.application.Transactions;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.port.LedgerStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class HoldReleaseTest extends ApiTest {

    @Autowired
    LedgerStore store;

    @Autowired
    Transactions transactions;

    void hold(String accountId, String ref) {
        new PaymentTransitions(store, transactions, Clock.systemUTC())
                .hold(new AccountId(accountId), "NullPointerException", ref, Actor.COMPLETION_SERVICE);
    }

    @Test
    void heldAccountRefusesUserAndAmlOperationsButAllowsCancel() {
        var id = account(100);
        var paymentId = withdraw(id, UUID.randomUUID().toString(), 30).text("payment_id");
        hold(id, "verification-results/3/17");

        assertError(deposit(id, UUID.randomUUID().toString(), 5, "tx-h-" + id), 409, "account_on_hold");
        assertError(withdraw(id, UUID.randomUUID().toString(), 5), 409, "account_on_hold");
        assertError(post("/internal/accounts/" + id + "/block", "{\"reason\":\"aml\"}"), 409, "account_on_hold");
        assertError(post("/internal/accounts/" + id + "/unblock", "{\"reason\":\"aml\"}"), 409, "account_on_hold");

        assertThat(post("/payments/" + paymentId + "/cancel", null).text("status")).isEqualTo("CANCELLED");
        assertThat(balance(id)).isEqualTo(100);
        assertThat(status(id)).isEqualTo("ON_HOLD");
    }

    @Test
    void releaseSetsTheChosenStatus() {
        var id = account(10);
        assertError(release(id, "ACTIVE", false), 409, "account_not_on_hold");
        hold(id, "ref-1");

        assertError(release(id, "ON_HOLD", false), 400, "invalid_request");
        assertThat(release(id, "BLOCKED", false).text("status")).isEqualTo("BLOCKED");
    }

    @Test
    void releaseToActiveOverPriorBlockNeedsConfirm() {
        var id = account(10);
        post("/internal/accounts/" + id + "/block", "{\"reason\":\"aml\"}");
        hold(id, "ref-1");
        hold(id, "ref-2");
        hold(id, "ref-2");

        assertError(release(id, "ACTIVE", false), 409, "confirm_required");
        var released = release(id, "ACTIVE", true);
        assertThat(released.text("status")).isEqualTo("ACTIVE");
        assertThat(released.number("version")).isEqualTo(6);
    }

    Resp release(String id, String to, boolean confirm) {
        return post("/internal/accounts/" + id + "/release", """
                {"to":"%s","confirm":%s,"reason":"fixed"}""".formatted(to, confirm));
    }
}
