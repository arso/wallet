package org.arso.wallet.worker;

import org.apache.kafka.common.errors.TimeoutException;
import org.arso.wallet.events.VerificationResult;
import org.arso.wallet.events.json.EventJson;
import org.arso.wallet.ledger.application.LockRetriesExhaustedException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.util.concurrent.ExecutionException;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static org.arso.wallet.worker.ErrorClassifier.Kind.*;
import static org.assertj.core.api.Assertions.assertThat;

class ErrorClassifierTest {

    final ErrorClassifier classifier = ErrorClassifier.defaults();

    @Test
    void infrastructureErrorsAreTransient() {
        assertThat(classifier.classify(dynamo(HTTP_BAD_REQUEST, "ExpiredTokenException"))).isEqualTo(TRANSIENT);
        assertThat(classifier.classify(dynamo(HTTP_BAD_REQUEST, "ResourceNotFoundException"))).isEqualTo(TRANSIENT);
        assertThat(classifier.classify(dynamo(HTTP_INTERNAL_ERROR, "InternalServerError"))).isEqualTo(TRANSIENT);
        assertThat(classifier.classify(new RuntimeException(SdkClientException.create("timeout")))).isEqualTo(TRANSIENT);
        assertThat(classifier.classify(new ExecutionException(new TimeoutException("kafka")))).isEqualTo(TRANSIENT);
        assertThat(classifier.classify(new LockRetriesExhaustedException(3))).isEqualTo(TRANSIENT);
    }

    @Test
    void recordErrorsArePermanent() {
        var parse = catchThrowable(() -> EventJson.read("nope".getBytes(), VerificationResult.class));

        assertThat(classifier.classify(parse)).isEqualTo(PERMANENT);
        assertThat(classifier.classify(new PermanentRecordException("schema_version 7"))).isEqualTo(PERMANENT);
        assertThat(classifier.classify(dynamo(HTTP_BAD_REQUEST, "ValidationException"))).isEqualTo(PERMANENT);
    }

    @Test
    void bugsAreUnknown() {
        assertThat(classifier.classify(new NullPointerException())).isEqualTo(UNKNOWN);
        assertThat(classifier.classify(dynamo(HTTP_BAD_REQUEST, "ConditionalCheckFailedException"))).isEqualTo(UNKNOWN);
    }

    @Test
    void appTypesComeFirst() {
        var custom = classifier.withPermanent(IllegalStateException.class).withTransient(UnsupportedOperationException.class);

        assertThat(custom.classify(new IllegalStateException())).isEqualTo(PERMANENT);
        assertThat(custom.classify(new UnsupportedOperationException())).isEqualTo(TRANSIENT);
    }

    static DynamoDbException dynamo(int status, String code) {
        return (DynamoDbException) DynamoDbException.builder()
                .statusCode(status)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build())
                .build();
    }

    static Throwable catchThrowable(Runnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("expected an exception");
        } catch (RuntimeException e) {
            return e;
        }
    }
}
