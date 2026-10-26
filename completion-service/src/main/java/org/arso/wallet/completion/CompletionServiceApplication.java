package org.arso.wallet.completion;

import org.arso.wallet.completion.config.CompletionConfig;
import org.arso.wallet.completion.config.CompletionProperties;
import org.arso.wallet.completion.config.LedgerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootConfiguration
@EnableAutoConfiguration
@EnableConfigurationProperties(CompletionProperties.class)
@Import({LedgerConfig.class, CompletionConfig.class})
public class CompletionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CompletionServiceApplication.class, args);
    }
}
