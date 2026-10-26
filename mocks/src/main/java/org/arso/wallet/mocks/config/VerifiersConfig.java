package org.arso.wallet.mocks.config;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.arso.wallet.events.Events;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.mocks.adapter.kafka.KafkaResultPublisher;
import org.arso.wallet.mocks.adapter.kafka.VerifierHandler;
import org.arso.wallet.mocks.application.Verifiers;
import org.arso.wallet.worker.AckedProducer;
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
@ConditionalOnBooleanProperty("mocks.verifiers.enabled")
public class VerifiersConfig {

    @Bean(destroyMethod = "close")
    AckedProducer verifierProducer(MocksProperties properties) {
        return new AckedProducer(properties.kafkaBootstrapServers(), Map.of());
    }

    @Bean
    Verifiers verifiers(AckedProducer verifierProducer, MocksProperties properties) {
        var config = properties.verifiers();
        return new Verifiers(
                new KafkaResultPublisher(verifierProducer), config.minLatency(), config.maxLatency(), Clock.systemUTC());
    }

    @Bean
    SmartLifecycle verifierPools(Verifiers verifiers, MocksProperties properties, MeterRegistry meters) {
        var config = properties.verifiers();
        var runners = config.steps().stream().map(step -> new PoolRunner(new WorkerPool(
                        WorkerPool.consumerConfig(properties.kafkaBootstrapServers(), "verifier-" + step, null),
                        WorkerSettings.of("verifier-" + step, Actor.INTERNAL, Events.WITHDRAW_REQUESTS, Mode.UNORDERED,
                                        HoldTarget.PAYMENT_ID)
                                .withMaxInFlight(config.maxInFlight()),
                        new VerifierHandler(step, verifiers),
                        ErrorClassifier.defaults(),
                        PoolRunner.NO_HOLDS,
                        meters)))
                .toList();
        return new Runners(runners);
    }

    record Runners(List<PoolRunner> runners) implements SmartLifecycle {

        @Override
        public void start() {
            runners.forEach(PoolRunner::start);
        }

        @Override
        public void stop() {
            runners.forEach(PoolRunner::stop);
        }

        @Override
        public boolean isRunning() {
            return runners.stream().anyMatch(PoolRunner::isRunning);
        }
    }
}
