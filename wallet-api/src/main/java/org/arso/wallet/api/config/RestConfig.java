package org.arso.wallet.api.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.arso.wallet.api.adapter.rest.WalletController;
import org.arso.wallet.api.application.ApiErrors;
import org.arso.wallet.api.application.Idempotency;
import org.arso.wallet.api.application.WalletCommands;
import org.arso.wallet.api.application.WalletQueries;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
public class RestConfig {

    @Bean
    WalletController walletController(
            WalletCommands commands,
            WalletQueries queries,
            Idempotency idempotency,
            ApiErrors errors,
            JsonMapper mapper,
            MeterRegistry meters) {
        return new WalletController(commands, queries, idempotency, errors, mapper, meters);
    }
}
