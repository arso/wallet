package org.arso.wallet.api.port;

import org.arso.wallet.ledger.domain.Counterparty;

import java.util.Optional;

public interface BeneficiaryLookup {

    /**
     * @throws BeneficiaryUnavailableException when the beneficiary service cannot answer
     */
    Optional<Counterparty> find(String beneficiaryId);
}
