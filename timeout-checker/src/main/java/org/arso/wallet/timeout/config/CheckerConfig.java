package org.arso.wallet.timeout.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.arso.wallet.events.Events;
import org.arso.wallet.ledger.application.CrashPoint;
import org.arso.wallet.ledger.application.PaymentTransitions;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.port.AccountHolds;
import org.arso.wallet.ledger.port.LedgerStore;
import org.arso.wallet.timeout.adapter.kafka.KafkaTransferPublisher;
import org.arso.wallet.timeout.adapter.kafka.WithdrawRequestHandler;
import org.arso.wallet.timeout.application.TimeoutChecker;
import org.arso.wallet.timeout.port.TransferPublisher;
import org.arso.wallet.worker.AckedProducer;
import org.arso.wallet.worker.ErrorClassifier;
import org.arso.wallet.worker.WorkerPool;
import org.arso.wallet.worker.WorkerSettings;
import org.arso.wallet.worker.WorkerSettings.HoldTarget;
import org.arso.wallet.worker.WorkerSettings.Mode;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
public class CheckerConfig {

    @Bean
    CrashPoint crashPoint() {
        return CrashPoint.NONE;
    }

    @Bean(destroyMethod = "close")
    AckedProducer ackedProducer(CheckerProperties properties, ObservationRegistry observations, MeterRegistry meters) {
        return new AckedProducer(properties.kafka().bootstrapServers(), Map.of(), observations, meters);
    }

    @Bean
    TransferPublisher transferPublisher(AckedProducer producer, MeterRegistry meters) {
        return new KafkaTransferPublisher(producer, meters);
    }

    @Bean
    TimeoutChecker timeoutChecker(
            LedgerStore store,
            PaymentTransitions transitions,
            TransferPublisher publisher,
            CrashPoint crashPoint,
            Clock clock,
            CheckerProperties properties) {
        return new TimeoutChecker(store, transitions, publisher, crashPoint, clock, properties.executingStuckAfter());
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public WorkerPool checkerPool(
            TimeoutChecker checker,
            AccountHolds holds,
            CheckerProperties properties,
            MeterRegistry meters,
            ObservationRegistry observations) {
        var kafka = properties.kafka();
        var workers = properties.workers();
        var handler = new WithdrawRequestHandler(checker, properties.verificationTimeout());
        var settings = WorkerSettings.of(
                        "timeout-checker", Actor.TIMEOUT_CHECKER, Events.WITHDRAW_REQUESTS, Mode.DELAYED,
                        HoldTarget.PAYMENT_ID)
                .withDueAt(handler::dueAt)
                .withMaxInFlight(workers.maxInFlight())
                .withUnknownBudget(workers.unknownMaxAttempts(), workers.unknownMaxElapsed());
        return new WorkerPool(
                WorkerPool.consumerConfig(kafka.bootstrapServers(), kafka.groupId(), kafka.instanceId()),
                settings,
                handler,
                ErrorClassifier.defaults(),
                holds,
                meters,
                observations);
    }

    @Bean
    HealthIndicator workerPoolHealthIndicator(WorkerPool pool) {
        return () -> pool.isAlive() ? Health.up().build() : Health.down().build();
    }
}
