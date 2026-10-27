package org.arso.wallet.mocks.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.arso.wallet.events.Events;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.mocks.adapter.kafka.AmlHandler;
import org.arso.wallet.mocks.adapter.wallet.HttpWalletAdmin;
import org.arso.wallet.mocks.application.AmlMonitor;
import org.arso.wallet.mocks.port.WalletAdmin.WalletUnavailableException;
import org.arso.wallet.worker.ErrorClassifier;
import org.arso.wallet.worker.WorkerPool;
import org.arso.wallet.worker.WorkerSettings;
import org.arso.wallet.worker.WorkerSettings.HoldTarget;
import org.arso.wallet.worker.WorkerSettings.Mode;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnBooleanProperty("mocks.aml.enabled")
public class AmlConfig {

    @Bean
    SmartLifecycle amlPool(MocksProperties properties, MeterRegistry meters, ObservationRegistry observations) {
        var config = properties.aml();
        var monitor = new AmlMonitor(new HttpWalletAdmin(config.walletUrl(), config.timeout()), config.blockThreshold());
        return new PoolRunner(new WorkerPool(
                WorkerPool.consumerConfig(properties.kafkaBootstrapServers(), "aml-mock", null),
                WorkerSettings.of("aml-mock", Actor.AML, Events.LEDGER_EVENTS, Mode.KEYED, HoldTarget.ACCOUNT_ID),
                new AmlHandler(monitor),
                ErrorClassifier.defaults().withTransient(WalletUnavailableException.class),
                PoolRunner.NO_HOLDS,
                meters,
                observations));
    }
}
