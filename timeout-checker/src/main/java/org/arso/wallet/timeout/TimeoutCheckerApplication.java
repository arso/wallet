package org.arso.wallet.timeout;

import org.arso.wallet.timeout.config.CheckerConfig;
import org.arso.wallet.timeout.config.CheckerProperties;
import org.arso.wallet.timeout.config.LedgerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootConfiguration
@EnableAutoConfiguration
@EnableConfigurationProperties(CheckerProperties.class)
@Import({LedgerConfig.class, CheckerConfig.class})
public class TimeoutCheckerApplication {

    public static void main(String[] args) {
        SpringApplication.run(TimeoutCheckerApplication.class, args);
    }
}
