package org.arso.wallet.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

final class Api {

    record Response(int status, JsonNode body) {

        String text(String field) {
            return body.path(field).asString();
        }
    }

    static final Set<String> FINAL = Set.of("COMPLETED", "REJECTED", "CANCELLED", "NEEDS_REVIEW");

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private Api() {}

    static String createAccount() {
        var created = post("/accounts", "{}");
        assertThat(created.status()).isEqualTo(201);
        return created.text("account_id");
    }

    static String account(long balance) {
        var id = createAccount();
        if (balance > 0) {
            assertThat(deposit(id, balance).status()).isEqualTo(201);
        }
        return id;
    }

    static Response deposit(String account, long amount) {
        return deposit(account, amount, UUID.randomUUID().toString());
    }

    static Response deposit(String account, long amount, String key) {
        return post("/accounts/" + account + "/deposits", key, """
                {"amount":%d,"currency":"USD","external_tx_id":"tx-%s",
                 "payer":{"payer_id":"p-1","name":"John","account_masked":"****1234",
                          "account_fingerprint":"hmac:1","country":"PL"}}""".formatted(amount, key));
    }

    static Response withdraw(String account, long amount, String beneficiary) {
        return withdraw(account, amount, beneficiary, UUID.randomUUID().toString());
    }

    static Response withdraw(String account, long amount, String beneficiary, String key) {
        return post("/accounts/" + account + "/withdrawals", key,
                "{\"amount\":%d,\"currency\":\"USD\",\"beneficiary_id\":\"%s\"}".formatted(amount, beneficiary));
    }

    static String withdrawal(String account, long amount, String beneficiary) {
        var accepted = retryWhileBusy(UUID.randomUUID().toString(), key -> withdraw(account, amount, beneficiary, key));
        assertThat(accepted.status()).as(accepted.body().toString()).isEqualTo(202);
        return accepted.text("payment_id");
    }

    private static Response retryWhileBusy(String key, Function<String, Response> request) {
        return await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(50)).atMost(Duration.ofSeconds(30))
                .until(() -> request.apply(key), response -> response.status() != 503);
    }

    static Response cancel(String payment) {
        return post("/payments/" + payment + "/cancel", "{\"reason\":\"e2e\"}");
    }

    static JsonNode payment(String id) {
        return get("/payments/" + id).body();
    }

    static long balance(String account) {
        return get("/accounts/" + account).body().path("balance").asLong();
    }

    static List<JsonNode> ledger(String account) {
        var entries = new ArrayList<JsonNode>();
        var path = "/accounts/" + account + "/ledger?limit=100";
        while (path != null) {
            var page = get(path).body();
            page.path("entries").forEach(entries::add);
            var next = page.path("next_before_seq");
            path = next.isNumber() ? "/accounts/" + account + "/ledger?limit=100&before_seq=" + next.asLong() : null;
        }
        return entries.reversed();
    }

    static JsonNode awaitPayment(String id, Predicate<JsonNode> condition) {
        return await().atMost(Duration.ofSeconds(90)).until(() -> payment(id), condition);
    }

    static JsonNode awaitFinal(String id) {
        return awaitPayment(id, p -> FINAL.contains(p.path("status").asString()));
    }

    static Response get(String path) {
        return send(HttpRequest.newBuilder(Stack.wallet().resolve(path)).GET());
    }

    static Response post(String path, String json) {
        return post(path, UUID.randomUUID().toString(), json);
    }

    static Response post(String path, String key, String json) {
        return send(HttpRequest.newBuilder(Stack.wallet().resolve(path))
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", key)
                .POST(HttpRequest.BodyPublishers.ofString(json)));
    }

    private static Response send(HttpRequest.Builder request) {
        try {
            var response = HTTP.send(request.timeout(Duration.ofSeconds(30)).build(), HttpResponse.BodyHandlers.ofString());
            var body = response.body().isEmpty() ? JSON.createObjectNode() : JSON.readTree(response.body());
            return new Response(response.statusCode(), body);
        } catch (Exception e) {
            throw new IllegalStateException(request.build().uri().toString(), e);
        }
    }
}
