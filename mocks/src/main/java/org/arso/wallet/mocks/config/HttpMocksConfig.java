package org.arso.wallet.mocks.config;

import org.arso.wallet.mocks.adapter.rest.MockController;
import org.arso.wallet.mocks.application.BeneficiaryDirectory;
import org.arso.wallet.mocks.application.TransferDesk;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class HttpMocksConfig {

    @Bean
    MockController mockController(MocksProperties properties) {
        return new MockController(new BeneficiaryDirectory(), new TransferDesk(properties.transfer().timeoutDelay()));
    }
}
