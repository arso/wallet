package org.arso.wallet.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.arso.wallet.testing.DynamoDbLocal;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(classes = WalletApiApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
abstract class ApiTest {

    record Resp(int status, JsonNode body) {

        String text(String field) {
            return body.path(field).asString();
        }

        long number(String field) {
            return body.path(field).asLong();
        }
    }

    @LocalServerPort
    int port;

    @Autowired
    JsonMapper mapper;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("wallet.dynamodb.endpoint", () -> DynamoDbLocal.endpoint().toString());
        registry.add("wallet.dynamodb.table-name", () -> "wallet-api-test-" + UUID.randomUUID());
        registry.add("wallet.beneficiary.url", () -> BeneficiaryStub.url().toString());
        registry.add("wallet.verification.steps", () -> "aml,kyc");
        registry.add("wallet.lock.max-attempts", () -> "10");
    }

    Resp post(String path, String key, String json) {
        var request = client().post().uri(path).contentType(MediaType.APPLICATION_JSON);
        if (key != null) {
            request.header("Idempotency-Key", key);
        }
        if (json != null) {
            request.body(json);
        }
        return request.exchange((_, response) -> read(response.getStatusCode(), response.bodyTo(String.class)));
    }

    Resp post(String path, String json) {
        return post(path, UUID.randomUUID().toString(), json);
    }

    Resp get(String path) {
        return client().get().uri(path)
                .exchange((_, response) -> read(response.getStatusCode(), response.bodyTo(String.class)));
    }

    String createAccount() {
        var created = post("/accounts", "{}");
        assertThat(created.status()).isEqualTo(201);
        return created.text("account_id");
    }

    String account(long balance) {
        var id = createAccount();
        if (balance > 0) {
            assertThat(deposit(id, UUID.randomUUID().toString(), balance, "tx-" + UUID.randomUUID()).status())
                    .isEqualTo(201);
        }
        return id;
    }

    Resp deposit(String accountId, String key, long amount, String externalTxId) {
        return post("/accounts/" + accountId + "/deposits", key, """
                {"amount":%d,"currency":"USD","external_tx_id":"%s",
                 "payer":{"payer_id":"p-1","name":"John","account_masked":"****1234",
                          "account_fingerprint":"hmac:1","country":"PL"}}""".formatted(amount, externalTxId));
    }

    Resp withdraw(String accountId, String key, long amount) {
        return post("/accounts/" + accountId + "/withdrawals", key, """
                {"amount":%d,"currency":"USD","beneficiary_id":"b-1"}""".formatted(amount));
    }

    long balance(String accountId) {
        return get("/accounts/" + accountId).number("balance");
    }

    String status(String accountId) {
        return get("/accounts/" + accountId).text("status");
    }

    private RestClient client() {
        return RestClient.create("http://localhost:" + port);
    }

    private Resp read(HttpStatusCode status, String body) {
        return new Resp(status.value(), body == null || body.isEmpty() ? mapper.createObjectNode() : mapper.readTree(body));
    }
}
