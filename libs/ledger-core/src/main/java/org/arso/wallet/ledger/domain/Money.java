package org.arso.wallet.ledger.domain;

import static org.arso.wallet.ledger.domain.DomainException.Code.CURRENCY_MISMATCH;
import static org.arso.wallet.ledger.domain.DomainException.Code.INSUFFICIENT_FUNDS;
import static org.arso.wallet.ledger.domain.DomainException.Code.INVALID_AMOUNT;

import java.util.Objects;

public record Money(long amountMinor, Currency currency) {

    public Money {
        Objects.requireNonNull(currency);
        if (amountMinor < 0) {
            throw new DomainException(INVALID_AMOUNT, "amount must not be negative");
        }
    }

    public static Money zero(Currency currency) {
        return new Money(0, currency);
    }

    public static Money positive(long amountMinor, Currency currency) {
        if (amountMinor <= 0) {
            throw new DomainException(INVALID_AMOUNT, "amount must be positive");
        }
        return new Money(amountMinor, currency);
    }

    public Money plus(Money other) {
        requireSameCurrency(other);
        try {
            return new Money(Math.addExact(amountMinor, other.amountMinor), currency);
        } catch (ArithmeticException e) {
            throw new DomainException(INVALID_AMOUNT, "amount too large");
        }
    }

    public Money minus(Money other) {
        requireSameCurrency(other);
        if (other.amountMinor > amountMinor) {
            throw new DomainException(INSUFFICIENT_FUNDS, "insufficient funds");
        }
        return new Money(amountMinor - other.amountMinor, currency);
    }

    private void requireSameCurrency(Money other) {
        if (!currency.equals(other.currency)) {
            throw new DomainException(CURRENCY_MISMATCH, "expected " + currency + ", got " + other.currency);
        }
    }
}
