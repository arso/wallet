package org.arso.wallet.ledger.domain;

import static org.arso.wallet.ledger.domain.DomainException.Code.NOT_FOUND;

public record PaymentId(String value) {

    public PaymentId {
        if (!Ulid.isValid(value)) {
            throw new DomainException(NOT_FOUND, "unknown payment " + value);
        }
    }

    public static PaymentId random() {
        return new PaymentId(Ulid.next());
    }

    @Override
    public String toString() {
        return value;
    }
}
