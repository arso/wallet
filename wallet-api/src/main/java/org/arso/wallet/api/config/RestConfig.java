package org.arso.wallet.api.config;

import org.arso.wallet.api.adapter.rest.WalletHandlers;
import org.arso.wallet.api.application.ApiErrors;
import org.arso.wallet.api.application.Idempotency;
import org.arso.wallet.api.application.WalletCommands;
import org.arso.wallet.api.application.WalletQueries;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
public class RestConfig {

    @Bean
    RouterFunction<ServerResponse> walletRoutes(
            WalletCommands commands, WalletQueries queries, Idempotency idempotency, ApiErrors errors, JsonMapper mapper) {
        return new WalletHandlers(commands, queries, idempotency, errors, mapper).routes();
    }
}
