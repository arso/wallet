package org.arso.wallet.api.adapter.rest;

import static org.arso.wallet.ledger.domain.DomainException.Code.INVALID_REQUEST;

import org.arso.wallet.ledger.domain.AccountStatus;
import org.arso.wallet.ledger.domain.Counterparty;
import org.arso.wallet.ledger.domain.Currency;
import org.arso.wallet.ledger.domain.DomainException;
import org.arso.wallet.ledger.domain.Money;

final class Bodies {

    private Bodies() {}

    record CreateAccount(String currency) {

        Currency toCurrency() {
            return currency == null ? null : new Currency(currency);
        }
    }

    record Deposit(Long amount, String currency, String externalTxId, Payer payer) {

        Counterparty toPayer() {
            if (payer == null || externalTxId == null || externalTxId.isBlank()) {
                throw new DomainException(INVALID_REQUEST, "external_tx_id and payer are required");
            }
            return new Counterparty(
                    payer.payerId, payer.name, payer.accountMasked, payer.accountFingerprint, payer.country);
        }
    }

    record Payer(String payerId, String name, String accountMasked, String accountFingerprint, String country) {}

    record Withdraw(Long amount, String currency, String beneficiaryId) {

        String requireBeneficiary() {
            if (beneficiaryId == null || beneficiaryId.isBlank()) {
                throw new DomainException(INVALID_REQUEST, "beneficiary_id is required");
            }
            return beneficiaryId;
        }
    }

    record Reason(String reason) {}

    record Release(String to, Boolean confirm, String reason) {

        AccountStatus target() {
            try {
                return AccountStatus.valueOf(to);
            } catch (IllegalArgumentException | NullPointerException e) {
                throw new DomainException(INVALID_REQUEST, "to must be ACTIVE or BLOCKED");
            }
        }
    }

    static Money money(Long amount, String currency) {
        if (amount == null) {
            throw new DomainException(INVALID_REQUEST, "amount is required");
        }
        return Money.positive(amount, currency == null ? Currency.USD : new Currency(currency));
    }
}
