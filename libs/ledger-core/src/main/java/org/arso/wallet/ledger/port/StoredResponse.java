package org.arso.wallet.ledger.port;

public record StoredResponse(String requestHash, int status, String body) {
}
