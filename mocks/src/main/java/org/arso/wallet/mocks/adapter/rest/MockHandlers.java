package org.arso.wallet.mocks.adapter.rest;

import java.util.LinkedHashMap;
import java.util.Map;
import org.arso.wallet.mocks.application.BeneficiaryDirectory;
import org.arso.wallet.mocks.application.TransferDesk;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

public final class MockHandlers {

    record TransferRequest(String paymentId, String accountId, Long amount, String currency, String beneficiaryId) {}

    private final BeneficiaryDirectory beneficiaries;
    private final TransferDesk transfers;

    public MockHandlers(BeneficiaryDirectory beneficiaries, TransferDesk transfers) {
        this.beneficiaries = beneficiaries;
        this.transfers = transfers;
    }

    public RouterFunction<ServerResponse> routes() {
        return RouterFunctions.route()
                .GET("/beneficiaries/{id}", this::beneficiary)
                .POST("/transfers", this::transfer)
                .GET("/transfers/{key}", this::attempts)
                .build();
    }

    private ServerResponse beneficiary(ServerRequest req) {
        return beneficiaries.find(req.pathVariable("id"))
                .map(b -> json(HttpStatus.OK, b))
                .orElseGet(() -> ServerResponse.notFound().build());
    }

    private ServerResponse transfer(ServerRequest req) throws Exception {
        var key = req.headers().firstHeader("Idempotency-Key");
        var body = req.body(TransferRequest.class);
        if (key == null || body.beneficiaryId() == null) {
            return json(HttpStatus.BAD_REQUEST, Map.of("error", "invalid_request", "message", "Idempotency-Key and beneficiary_id are required"));
        }
        return switch (transfers.transfer(key, body.beneficiaryId())) {
            case TransferDesk.Result.Sent(var externalTxId) -> json(HttpStatus.OK, Map.of("external_tx_id", externalTxId));
            case TransferDesk.Result.Rejected(var reason) -> json(HttpStatus.UNPROCESSABLE_CONTENT, Map.of("error", "transfer_rejected", "message", reason));
        };
    }

    private ServerResponse attempts(ServerRequest req) {
        var key = req.pathVariable("key");
        var body = new LinkedHashMap<String, Object>();
        body.put("attempts", transfers.attempts(key));
        if (transfers.result(key) instanceof TransferDesk.Result.Sent(var externalTxId)) {
            body.put("external_tx_id", externalTxId);
        }
        return json(HttpStatus.OK, body);
    }

    private static ServerResponse json(HttpStatus status, Object body) {
        return ServerResponse.status(status).contentType(MediaType.APPLICATION_JSON).body(body);
    }
}
