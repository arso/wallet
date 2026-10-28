package org.arso.wallet.worker;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.arso.wallet.ledger.application.LockRetriesExhaustedException;
import org.arso.wallet.ledger.port.TransientStoreException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import tools.jackson.core.JacksonException;

import java.util.ArrayList;
import java.util.List;

public final class ErrorClassifier {

    public enum Kind {
        TRANSIENT,
        PERMANENT,
        UNKNOWN
    }

    private final List<Class<? extends Throwable>> transientTypes;
    private final List<Class<? extends Throwable>> permanentTypes;

    private ErrorClassifier(List<Class<? extends Throwable>> transientTypes, List<Class<? extends Throwable>> permanentTypes) {
        this.transientTypes = transientTypes;
        this.permanentTypes = permanentTypes;
    }

    public static ErrorClassifier defaults() {
        return new ErrorClassifier(List.of(), List.of());
    }

    @SafeVarargs
    public final ErrorClassifier withTransient(Class<? extends Throwable>... types) {
        var all = new ArrayList<>(transientTypes);
        all.addAll(List.of(types));
        return new ErrorClassifier(List.copyOf(all), permanentTypes);
    }

    @SafeVarargs
    public final ErrorClassifier withPermanent(Class<? extends Throwable>... types) {
        var all = new ArrayList<>(permanentTypes);
        all.addAll(List.of(types));
        return new ErrorClassifier(transientTypes, List.copyOf(all));
    }

    public Kind classify(Throwable error) {
        for (var e = error; e != null; e = e.getCause() == e ? null : e.getCause()) {
            var kind = known(e);
            if (kind != Kind.UNKNOWN) {
                return kind;
            }
        }
        return Kind.UNKNOWN;
    }

    private Kind known(Throwable e) {
        if (permanentTypes.stream().anyMatch(t -> t.isInstance(e))) {
            return Kind.PERMANENT;
        }
        if (transientTypes.stream().anyMatch(t -> t.isInstance(e))) {
            return Kind.TRANSIENT;
        }
        return switch (e) {
            case PermanentRecordException _, JacksonException _ -> Kind.PERMANENT;
            case SerializationException _, RecordTooLargeException _ -> Kind.PERMANENT;
            case TransactionCanceledException t when t.cancellationReasons().stream()
                    .anyMatch(r -> "ValidationError".equals(r.code())) -> Kind.PERMANENT;
            case DynamoDbException d when isValidation(d) -> Kind.PERMANENT;
            case DynamoDbException d when isConditionFailure(d) -> Kind.UNKNOWN;
            case SdkException _, KafkaException _ -> Kind.TRANSIENT;
            case TransientStoreException _, LockRetriesExhaustedException _ -> Kind.TRANSIENT;
            default -> Kind.UNKNOWN;
        };
    }

    private static boolean isValidation(DynamoDbException e) {
        return e.awsErrorDetails() != null && "ValidationException".equals(e.awsErrorDetails().errorCode());
    }

    private static boolean isConditionFailure(DynamoDbException e) {
        return e.awsErrorDetails() != null && "ConditionalCheckFailedException".equals(e.awsErrorDetails().errorCode());
    }
}
