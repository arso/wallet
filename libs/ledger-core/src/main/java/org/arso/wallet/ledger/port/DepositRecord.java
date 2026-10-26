package org.arso.wallet.ledger.port;

import org.arso.wallet.ledger.domain.AccountId;

public record DepositRecord(String externalTxId, AccountId accountId, long seq) {}
