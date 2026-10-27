package org.arso.wallet.mocks.config;

import org.arso.wallet.mocks.adapter.rest.MockHandlers;
import org.arso.wallet.mocks.application.BeneficiaryDirectory;
import org.arso.wallet.mocks.application.TransferDesk;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;

@Configuration(proxyBeanMethods = false)
public class HttpMocksConfig {

    @Bean
    RouterFunction<ServerResponse> mockRoutes(MocksProperties properties) {
        return new MockHandlers(new BeneficiaryDirectory(), new TransferDesk(properties.transfer().delay())).routes();
    }
}
