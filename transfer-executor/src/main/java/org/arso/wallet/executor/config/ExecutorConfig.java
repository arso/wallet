package org.arso.wallet.executor.config;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.core.IntervalFunction;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import java.net.http.HttpClient;
import org.arso.wallet.events.Events;
import org.arso.wallet.executor.adapter.kafka.TransferRequestHandler;
import org.arso.wallet.executor.adapter.transfer.HttpTransferService;
import org.arso.wallet.executor.adapter.transfer.ResilientTransferService;
import org.arso.wallet.executor.application.Executor;
import org.arso.wallet.executor.port.TransferService;
import org.arso.wallet.ledger.application.CrashPoint;
import org.arso.wallet.ledger.application.PaymentTransitions;
import org.arso.wallet.ledger.domain.Actor;
import org.arso.wallet.ledger.port.AccountHolds;
import org.arso.wallet.ledger.port.LedgerStore;
import org.arso.wallet.worker.ErrorClassifier;
import org.arso.wallet.worker.WorkerPool;
import org.arso.wallet.worker.WorkerSettings;
import org.arso.wallet.worker.WorkerSettings.HoldTarget;
import org.arso.wallet.worker.WorkerSettings.Mode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.http.client.observation.ClientRequestObservationContext;
import org.springframework.http.client.observation.DefaultClientRequestObservationConvention;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
public class ExecutorConfig {

    @Bean
    CrashPoint crashPoint() {
        return CrashPoint.NONE;
    }

    @Bean
    CircuitBreaker transferCircuit(ExecutorProperties properties, MeterRegistry meters) {
        var circuit = properties.circuit();
        var breaker = CircuitBreaker.of("transfer-service", CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(circuit.windowSize())
                .minimumNumberOfCalls(circuit.minimumCalls())
                .failureRateThreshold(circuit.failureRate())
                .waitDurationInOpenState(circuit.openWait())
                .permittedNumberOfCallsInHalfOpenState(circuit.halfOpenCalls())
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordResult(result -> result instanceof TransferService.Result.Unknown)
                .build());
        Gauge.builder("circuit_breaker_state", breaker, c -> switch (c.getState()) {
                    case CLOSED -> 0;
                    case HALF_OPEN -> 1;
                    default -> 2;
                })
                .tag("name", breaker.getName())
                .register(meters);
        return breaker;
    }

    @Bean
    TransferService transferService(
            ExecutorProperties properties, CircuitBreaker circuit, ObservationRegistry observations) {
        var transfer = properties.transfer();
        var http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(transfer.connectTimeout())
                .build();
        var factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(transfer.readTimeout());
        var client = RestClient.builder()
                .baseUrl(transfer.url().toString())
                .requestFactory(factory)
                .observationRegistry(observations)
                .observationConvention(new DefaultClientRequestObservationConvention() {
                    @Override
                    public String getContextualName(ClientRequestObservationContext context) {
                        return super.getContextualName(context) + " " + uri(context).getValue();
                    }
                })
                .build();
        return new ResilientTransferService(
                new HttpTransferService(client),
                circuit,
                transfer.retries(),
                IntervalFunction.ofExponentialRandomBackoff(transfer.initialBackoff(), 2.0, 0.5));
    }

    @Bean
    Executor executor(LedgerStore store, PaymentTransitions transitions, TransferService transfers, CrashPoint crashPoint) {
        return new Executor(store, transitions, transfers, crashPoint);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    WorkerPool executorPool(
            Executor executor,
            AccountHolds holds,
            CircuitBreaker circuit,
            ExecutorProperties properties,
            MeterRegistry meters,
            ObservationRegistry observations) {
        var kafka = properties.kafka();
        var workers = properties.workers();
        var settings = WorkerSettings.of(
                        "transfer-executor", Actor.TRANSFER_EXECUTOR, Events.TRANSFER_REQUESTS, Mode.UNORDERED,
                        HoldTarget.PAYMENT_ID)
                .withMaxInFlight(workers.maxInFlight())
                .withUnknownBudget(workers.unknownMaxAttempts(), workers.unknownMaxElapsed());
        var pool = new WorkerPool(
                WorkerPool.consumerConfig(kafka.bootstrapServers(), kafka.groupId(), kafka.instanceId()),
                settings,
                new TransferRequestHandler(executor),
                ErrorClassifier.defaults().withTransient(CallNotPermittedException.class),
                holds,
                meters,
                observations);
        circuit.getEventPublisher().onStateTransition(
                e -> pool.circuitOpen(e.getStateTransition().getToState() == CircuitBreaker.State.OPEN));
        return pool;
    }
}
