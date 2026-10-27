package org.arso.wallet.api;

import com.jayway.jsonpath.JsonPath;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.arso.wallet.testing.DynamoDbLocal;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.test.web.servlet.client.RestTestClient.BodyContentSpec;
import org.springframework.test.web.servlet.client.RestTestClient.ResponseSpec;

@SpringBootTest(classes = WalletApiApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
abstract class ApiTest {

    @LocalServerPort
    int port;

    RestTestClient client;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("wallet.dynamodb.endpoint", () -> DynamoDbLocal.endpoint().toString());
        registry.add("wallet.dynamodb.table-name", () -> "wallet-api-test-" + UUID.randomUUID());
        registry.add("wallet.beneficiary.url", () -> BeneficiaryStub.url().toString());
        registry.add("wallet.verification.steps", () -> "aml,kyc");
        registry.add("wallet.lock.max-attempts", () -> "10");
    }

    @BeforeEach
    void client() {
        client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    ResponseSpec post(String path, String key, String json) {
        var request = client.post().uri(path).contentType(MediaType.APPLICATION_JSON);
        if (key != null) {
            request.header("Idempotency-Key", key);
        }
        return (json == null ? request : request.body(json)).exchange();
    }

    ResponseSpec post(String path, String json) {
        return post(path, UUID.randomUUID().toString(), json);
    }

    ResponseSpec get(String path) {
        return client.get().uri(path).exchange();
    }

    String createAccount() {
        return read(post("/accounts", "{}").expectStatus().isCreated(), "$.account_id");
    }

    String account(long balance) {
        var id = createAccount();
        if (balance > 0) {
            deposit(id, UUID.randomUUID().toString(), balance, "tx-" + UUID.randomUUID()).expectStatus().isCreated();
        }
        return id;
    }

    ResponseSpec deposit(String accountId, String key, long amount, String externalTxId) {
        return post("/accounts/" + accountId + "/deposits", key, """
                {"amount":%d,"currency":"USD","external_tx_id":"%s",
                 "payer":{"payer_id":"p-1","name":"John","account_masked":"****1234",
                          "account_fingerprint":"hmac:1","country":"PL"}}""".formatted(amount, externalTxId));
    }

    ResponseSpec withdraw(String accountId, String key, long amount) {
        return post("/accounts/" + accountId + "/withdrawals", key, """
                {"amount":%d,"currency":"USD","beneficiary_id":"b-1"}""".formatted(amount));
    }

    String pendingWithdrawal(String accountId, long amount) {
        return read(withdraw(accountId, UUID.randomUUID().toString(), amount).expectStatus().isAccepted(), "$.payment_id");
    }

    long balance(String accountId) {
        return ApiTest.<Number>read(get("/accounts/" + accountId).expectStatus().isOk(), "$.balance").longValue();
    }

    String status(String accountId) {
        return read(get("/accounts/" + accountId).expectStatus().isOk(), "$.status");
    }

    static <T> T read(ResponseSpec response, String path) {
        return JsonPath.read(response.returnResult(String.class).getResponseBody(), path);
    }

    static <T> T read(BodyContentSpec body, String path) {
        return JsonPath.read(new String(body.returnResult().getResponseBody(), StandardCharsets.UTF_8), path);
    }

    static void assertError(ResponseSpec response, HttpStatus status, String error) {
        response.expectStatus().isEqualTo(status)
                .expectHeader().contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.error").isEqualTo(error)
                .jsonPath("$.type").isEqualTo("about:blank")
                .jsonPath("$.status").isEqualTo(status.value())
                .jsonPath("$.title").isEqualTo(status.getReasonPhrase())
                .jsonPath("$.detail").exists();
    }
}
