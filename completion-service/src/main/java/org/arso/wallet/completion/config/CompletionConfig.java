package org.arso.wallet.completion.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.arso.wallet.completion.adapter.kafka.KafkaTransferPublisher;
import org.arso.wallet.completion.adapter.kafka.VerificationResultHandler;
import org.arso.wallet.completion.adapter.metrics.MicrometerCompletionMetrics;
import org.arso.wallet.completion.application.Completion;
import org.arso.wallet.completion.port.TransferPublisher;
import org.arso.wallet.events.Events;
import org.arso.wallet.ledger.application.CrashPoint;
import org.arso.wallet.ledger.application.PaymentTransitions;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.port.AccountHolds;
import org.arso.wallet.ledger.port.LedgerStore;
import org.arso.wallet.worker.AckedProducer;
import org.arso.wallet.worker.ErrorClassifier;
import org.arso.wallet.worker.WorkerPool;
import org.arso.wallet.worker.WorkerSettings;
import org.arso.wallet.worker.WorkerSettings.Dispatch;
import org.arso.wallet.worker.WorkerSettings.HoldTarget;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
public class CompletionConfig {

    @Bean
    CrashPoint crashPoint() {
        return CrashPoint.NONE;
    }

    @Bean(destroyMethod = "close")
    AckedProducer ackedProducer(CompletionProperties properties, ObservationRegistry observations, MeterRegistry meters) {
        return new AckedProducer(properties.kafka().bootstrapServers(), Map.of(), observations, meters);
    }

    @Bean
    TransferPublisher transferPublisher(AckedProducer producer) {
        return new KafkaTransferPublisher(producer);
    }

    @Bean
    Completion completion(LedgerStore store, PaymentTransitions transitions, TransferPublisher publisher, CrashPoint crashPoint, Clock clock, MeterRegistry meters) {
        return new Completion(store, transitions, publisher, crashPoint, clock, new MicrometerCompletionMetrics(meters));
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    WorkerPool completionPool(Completion completion, AccountHolds holds, CompletionProperties properties, MeterRegistry meters, ObservationRegistry observations) {
        var kafka = properties.kafka();
        var workers = properties.workers();
        var settings = WorkerSettings.of("completion-service", Actor.COMPLETION_SERVICE, Events.VERIFICATION_RESULTS, new Dispatch.Keyed(workers.count(), workers.queueCapacity()), HoldTarget.PAYMENT_ID).withUnknownErrorBudget(workers.unknownMaxAttempts(), workers.unknownMaxElapsed());
        return new WorkerPool(WorkerPool.consumerConfig(kafka.bootstrapServers(), kafka.groupId(), kafka.instanceId()), settings, new VerificationResultHandler(completion), ErrorClassifier.defaults(), holds, meters, observations);
    }

    @Bean
    HealthIndicator workerPoolHealthIndicator(WorkerPool pool) {
        return () -> pool.isAlive() ? Health.up().build() : Health.down().build();
    }
}
