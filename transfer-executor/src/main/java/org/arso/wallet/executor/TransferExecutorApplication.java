package org.arso.wallet.executor;

import org.arso.wallet.executor.config.ExecutorConfig;
import org.arso.wallet.executor.config.ExecutorProperties;
import org.arso.wallet.executor.config.LedgerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootConfiguration
@EnableAutoConfiguration
@EnableConfigurationProperties(ExecutorProperties.class)
@Import({LedgerConfig.class, ExecutorConfig.class})
public class TransferExecutorApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransferExecutorApplication.class, args);
    }
}
