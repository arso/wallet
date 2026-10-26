package org.arso.wallet.ledger.domain;

import static org.arso.wallet.ledger.domain.DomainException.Code.NOT_FOUND;

public record AccountId(String value) {

    public AccountId {
        if (!Ulid.isValid(value)) {
            throw new DomainException(NOT_FOUND, "unknown account " + value);
        }
    }

    public static AccountId random() {
        return new AccountId(Ulid.next());
    }

    @Override
    public String toString() {
        return value;
    }
}
