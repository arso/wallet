package org.arso.wallet.ledger.adapter.dynamodb;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.interceptor.*;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import java.time.Duration;

/**
 * {@code dynamodb_request_seconds} per operation and outcome (spec 15.4), including SDK retries.
 */
public final class DynamoDbMetrics implements ExecutionInterceptor {

    private static final ExecutionAttribute<Long> START = new ExecutionAttribute<>("metrics-start");

    private final MeterRegistry meters;

    public DynamoDbMetrics(MeterRegistry meters) {
        this.meters = meters;
    }

    @Override
    public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes attributes) {
        attributes.putAttribute(START, System.nanoTime());
    }

    @Override
    public void afterExecution(Context.AfterExecution context, ExecutionAttributes attributes) {
        record(attributes, "ok");
    }

    @Override
    public void onExecutionFailure(Context.FailedExecution context, ExecutionAttributes attributes) {
        record(attributes, outcome(context.exception()));
    }

    private void record(ExecutionAttributes attributes, String outcome) {
        var start = attributes.getAttribute(START);
        if (start == null) {
            return;
        }
        Timer.builder("dynamodb_request")
                .tags("operation", attributes.getAttribute(SdkExecutionAttribute.OPERATION_NAME), "outcome", outcome)
                .publishPercentileHistogram()
                .register(meters)
                .record(Duration.ofNanos(System.nanoTime() - start));
    }

    private static String outcome(Throwable e) {
        return switch (e) {
            case ConditionalCheckFailedException _ -> "conditional_failed";
            case TransactionCanceledException t when t.cancellationReasons().stream()
                    .anyMatch(r -> "TransactionConflict".equals(r.code())) -> "transaction_conflict";
            case TransactionCanceledException t when t.cancellationReasons().stream()
                    .anyMatch(r -> "ConditionalCheckFailed".equals(r.code())) -> "conditional_failed";
            case SdkServiceException s when s.isThrottlingException() -> "throttled";
            default -> "error";
        };
    }
}
