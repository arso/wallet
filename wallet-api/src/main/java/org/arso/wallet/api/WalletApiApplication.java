package org.arso.wallet.api;

import org.arso.wallet.api.config.ApplicationConfig;
import org.arso.wallet.api.config.DynamoDbConfig;
import org.arso.wallet.api.config.RestConfig;
import org.arso.wallet.api.config.WalletProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootConfiguration
@EnableAutoConfiguration
@EnableConfigurationProperties(WalletProperties.class)
@Import({DynamoDbConfig.class, ApplicationConfig.class, RestConfig.class})
public class WalletApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(WalletApiApplication.class, args);
    }
}
