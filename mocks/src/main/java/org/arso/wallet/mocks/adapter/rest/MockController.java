package org.arso.wallet.mocks.adapter.rest;

import org.arso.wallet.mocks.application.BeneficiaryDirectory;
import org.arso.wallet.mocks.application.TransferDesk;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class MockController {

    record TransferRequest(String paymentId, String accountId, Long amount, String currency, String beneficiaryId) {
    }

    private final BeneficiaryDirectory beneficiaries;
    private final TransferDesk transfers;

    public MockController(BeneficiaryDirectory beneficiaries, TransferDesk transfers) {
        this.beneficiaries = beneficiaries;
        this.transfers = transfers;
    }

    @GetMapping("/beneficiaries/{id}")
    ResponseEntity<?> beneficiary(@PathVariable String id) {
        return beneficiaries.find(id).<ResponseEntity<?>>map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/transfers")
    ResponseEntity<Map<String, String>> transfer(
            @RequestHeader(name = "Idempotency-Key", required = false) String key, @RequestBody TransferRequest body)
            throws InterruptedException {
        if (key == null || body.beneficiaryId() == null) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "invalid_request", "message", "Idempotency-Key and beneficiary_id are required"));
        }
        return switch (transfers.transfer(key, body.beneficiaryId())) {
            case TransferDesk.Result.Sent(var externalTxId) ->
                    ResponseEntity.ok(Map.of("external_tx_id", externalTxId));
            case TransferDesk.Result.Rejected(var reason) -> ResponseEntity.status(HttpStatus.UNPROCESSABLE_CONTENT)
                    .body(Map.of("error", "transfer_rejected", "message", reason));
        };
    }
}
