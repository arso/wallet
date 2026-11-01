package org.arso.wallet.api.application;

import org.arso.wallet.ledger.port.StoredResponse;
import org.arso.wallet.ledger.port.TxItem.PutIdempotencyRecord;

import java.time.Instant;

public record Idem(String key, String hash) {

    StoredResponse stored(ApiResponse response) {
        return new StoredResponse(hash, response.status(), response.body());
    }

    PutIdempotencyRecord put(ApiResponse response, Instant now) {
        return new PutIdempotencyRecord(key, stored(response), now);
    }
}
