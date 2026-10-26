package org.arso.wallet.mocks.adapter.wallet;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import org.arso.wallet.mocks.port.WalletAdmin;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

public final class HttpWalletAdmin implements WalletAdmin {

    private final RestClient client;

    public HttpWalletAdmin(URI baseUrl, Duration timeout) {
        var factory = new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(timeout).build());
        factory.setReadTimeout(timeout);
        this.client = RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
    }

    @Override
    public Result block(String accountId, String reason, String idempotencyKey) {
        return post("/internal/accounts/{id}/block", accountId, reason, idempotencyKey);
    }

    @Override
    public Result unblock(String accountId, String reason, String idempotencyKey) {
        return post("/internal/accounts/{id}/unblock", accountId, reason, idempotencyKey);
    }

    private Result post(String path, String accountId, String reason, String idempotencyKey) {
        try {
            return client.post()
                    .uri(path, accountId)
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("reason", reason))
                    .exchange((_, response) -> {
                        var status = response.getStatusCode().value();
                        if (status >= 500) {
                            throw new WalletUnavailableException("wallet-api returned " + status, null);
                        }
                        if (status < 300) {
                            return Result.APPLIED;
                        }
                        var body = response.bodyTo(String.class);
                        return body != null && body.contains("\"account_on_hold\"") ? Result.ON_HOLD : Result.NOT_APPLICABLE;
                    });
        } catch (RestClientException e) {
            throw new WalletUnavailableException("wallet-api call failed", e);
        }
    }
}
