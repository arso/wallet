package org.arso.wallet.api.config;

import io.micrometer.observation.ObservationRegistry;
import java.time.Clock;
import org.arso.wallet.api.adapter.beneficiary.HttpBeneficiaryLookup;
import org.arso.wallet.api.application.ApiErrors;
import org.arso.wallet.api.application.Idempotency;
import org.arso.wallet.api.application.WalletCommands;
import org.arso.wallet.api.application.WalletQueries;
import org.arso.wallet.api.port.BeneficiaryLookup;
import org.arso.wallet.api.port.JsonWriter;
import org.arso.wallet.ledger.application.Transactions;
import org.arso.wallet.ledger.domain.Currency;
import org.arso.wallet.ledger.port.LedgerMetrics;
import org.arso.wallet.ledger.port.LedgerStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
public class ApplicationConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    JsonWriter jsonWriter(JsonMapper mapper) {
        return mapper::writeValueAsString;
    }

    @Bean
    BeneficiaryLookup beneficiaryLookup(WalletProperties properties, JsonMapper mapper, ObservationRegistry observations) {
        var config = properties.beneficiary();
        return new HttpBeneficiaryLookup(config.url(), config.timeout(), mapper, observations);
    }

    @Bean
    ApiErrors apiErrors(JsonWriter json) {
        return new ApiErrors(json);
    }

    @Bean
    Idempotency idempotency(LedgerStore store, ApiErrors errors, Clock clock) {
        return new Idempotency(store, errors, clock);
    }

    @Bean
    WalletCommands walletCommands(
            LedgerStore store,
            Transactions transactions,
            BeneficiaryLookup beneficiaries,
            JsonWriter json,
            Clock clock,
            WalletProperties properties,
            LedgerMetrics ledgerMetrics) {
        var settings = new WalletCommands.Settings(
                properties.verification().steps(), new Currency(properties.defaultCurrency()));
        return new WalletCommands(store, transactions, beneficiaries, json, clock, settings, ledgerMetrics);
    }

    @Bean
    WalletQueries walletQueries(LedgerStore store) {
        return new WalletQueries(store);
    }
}
