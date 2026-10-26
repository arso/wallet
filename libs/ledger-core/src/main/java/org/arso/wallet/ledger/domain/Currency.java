package org.arso.wallet.ledger.domain;

import static org.arso.wallet.ledger.domain.DomainException.Code.INVALID_CURRENCY;

public record Currency(String code) {

    public static final Currency USD = new Currency("USD");

    public Currency {
        if (code == null || !code.matches("[A-Z]{3}")) {
            throw new DomainException(INVALID_CURRENCY, "invalid currency " + code);
        }
        try {
            java.util.Currency.getInstance(code);
        } catch (IllegalArgumentException e) {
            throw new DomainException(INVALID_CURRENCY, "invalid currency " + code);
        }
    }

    @Override
    public String toString() {
        return code;
    }
}
