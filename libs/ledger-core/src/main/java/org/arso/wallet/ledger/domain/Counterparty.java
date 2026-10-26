package org.arso.wallet.ledger.domain;

import static org.arso.wallet.ledger.domain.DomainException.Code.INVALID_REQUEST;

import java.util.stream.Stream;

public record Counterparty(String id, String name, String accountMasked, String accountFingerprint, String country) {

    public Counterparty {
        if (Stream.of(id, name, accountMasked, accountFingerprint, country).anyMatch(v -> v == null || v.isBlank())) {
            throw new DomainException(INVALID_REQUEST, "incomplete counterparty");
        }
    }
}
