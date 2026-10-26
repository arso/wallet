package org.arso.wallet.api.port;

import java.util.Optional;
import org.arso.wallet.ledger.domain.Counterparty;

public interface BeneficiaryLookup {

    /** @throws BeneficiaryUnavailableException when the beneficiary service cannot answer */
    Optional<Counterparty> find(String beneficiaryId);
}
