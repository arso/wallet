package org.arso.wallet.mocks;

import org.arso.wallet.mocks.config.AmlConfig;
import org.arso.wallet.mocks.config.HttpMocksConfig;
import org.arso.wallet.mocks.config.MocksProperties;
import org.arso.wallet.mocks.config.VerifiersConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootConfiguration
@EnableAutoConfiguration
@EnableConfigurationProperties(MocksProperties.class)
@Import({HttpMocksConfig.class, VerifiersConfig.class, AmlConfig.class})
public class MocksApplication {

    public static void main(String[] args) {
        SpringApplication.run(MocksApplication.class, args);
    }
}
