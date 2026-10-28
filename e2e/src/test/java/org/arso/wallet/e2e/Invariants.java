package org.arso.wallet.e2e;

import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

final class Invariants {

    private static final Map<String, List<JsonNode>> SEEN = new ConcurrentHashMap<>();

    private Invariants() {
    }

    /**
     * Call once every payment of the account is final.
     */
    static void check(String account) {
        var ledger = Api.ledger(account);
        var balance = 0L;
        var payments = new LinkedHashMap<String, List<String>>();
        for (int i = 0; i < ledger.size(); i++) {
            var entry = ledger.get(i);
            var type = entry.path("event_type").asString();
            assertThat(entry.path("seq").asLong()).as("seq gap-free from 1").isEqualTo(i + 1);
            balance += switch (type) {
                case "deposit", "user_cancel_withdraw", "reject_withdraw" -> entry.path("amount").asLong();
                case "withdraw" -> -entry.path("amount").asLong();
                default -> 0;
            };
            assertThat(entry.path("balance_after").asLong()).as("replay at seq " + (i + 1)).isEqualTo(balance);
            assertThat(balance).as("balance never negative").isNotNegative();
            if (entry.has("payment_id")) {
                payments.computeIfAbsent(entry.path("payment_id").asString(), _ -> new ArrayList<>()).add(type);
            }
        }
        assertThat(Api.balance(account)).as("sum of ledger entries = balance").isEqualTo(balance);
        payments.forEach(Invariants::checkPayment);

        var before = SEEN.put(account, ledger);
        if (before != null) {
            assertThat(ledger.subList(0, before.size())).as("ledger entries never change").isEqualTo(before);
        }
    }

    static void checkUnchanged() {
        SEEN.forEach((account, before) ->
                assertThat(Api.ledger(account).subList(0, before.size())).as(account).isEqualTo(before));
    }

    private static void checkPayment(String id, List<String> entries) {
        var status = Api.payment(id).path("status").asString();
        var finalEntry = switch (status) {
            case "COMPLETED" -> List.of("withdraw_completed");
            case "REJECTED" -> List.of("reject_withdraw");
            case "CANCELLED" -> List.of("user_cancel_withdraw");
            case "NEEDS_REVIEW" -> List.<String>of();
            default -> throw new AssertionError("payment " + id + " is not final: " + status);
        };
        var expected = new ArrayList<>(List.of("withdraw"));
        expected.addAll(finalEntry);
        assertThat(entries).as("ledger entries of payment " + id + " (" + status + ")").isEqualTo(expected);
    }
}
