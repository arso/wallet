package org.arso.wallet.executor.adapter.transfer;

import java.util.Map;
import org.arso.wallet.executor.port.TransferService;
import org.arso.wallet.ledger.domain.Payment;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

public final class HttpTransferService implements TransferService {

    private final RestClient client;

    public HttpTransferService(RestClient client) {
        this.client = client;
    }

    @Override
    public Result transfer(Payment payment) {
        var body = Map.of(
                "payment_id", payment.id().value(),
                "account_id", payment.accountId().value(),
                "amount", payment.amount().amountMinor(),
                "currency", payment.amount().currency().code(),
                "beneficiary_id", payment.beneficiaryId());
        try {
            return client.post()
                    .uri("/transfers")
                    .header("Idempotency-Key", payment.id().value())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .exchange((_, response) -> {
                        var status = response.getStatusCode();
                        if (status.is2xxSuccessful()) {
                            var answer = response.bodyTo(Map.class);
                            return answer != null && answer.get("external_tx_id") instanceof String id
                                    ? new Result.Sent(id)
                                    : new Result.Unknown("no external_tx_id");
                        }
                        return status.isSameCodeAs(HttpStatus.UNPROCESSABLE_CONTENT)
                                ? new Result.Rejected(HttpStatus.UNPROCESSABLE_CONTENT.toString())
                                : new Result.Unknown("http " + status.value());
                    });
        } catch (RestClientException e) {
            return new Result.Unknown(e.getClass().getSimpleName());
        }
    }
}
