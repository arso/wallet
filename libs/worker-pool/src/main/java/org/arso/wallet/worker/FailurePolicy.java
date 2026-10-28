package org.arso.wallet.worker;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.arso.wallet.ledger.port.AccountHolds;
import org.arso.wallet.worker.RecordHandler.Outcome;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.WARNING;

final class FailurePolicy {

    private static final System.Logger LOG = System.getLogger(FailurePolicy.class.getName());

    private final RecordHandler handler;
    private final ErrorClassifier classifier;
    private final AccountHolds holds;
    private final WorkerSettings settings;
    private final MeterRegistry meters;

    FailurePolicy(RecordHandler handler, ErrorClassifier classifier, AccountHolds holds, WorkerSettings settings, MeterRegistry meters) {
        this.handler = handler;
        this.classifier = classifier;
        this.holds = holds;
        this.settings = settings;
        this.meters = meters;
    }

    Outcome run(ConsumerRecord<String, byte[]> record) {
        var start = System.nanoTime();
        var unknown = 0;
        for (int attempt = 1; ; attempt++) {
            try {
                return handler.handle(record);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (Exception e) {
                switch (classifier.classify(e)) {
                    case TRANSIENT -> meters.counter("transient_retries_total", "service", settings.service(),
                            "error_class", e.getClass().getSimpleName()).increment();
                    case PERMANENT -> {
                        return hold(record, e) ? Outcome.DONE : null;
                    }
                    case UNKNOWN -> {
                        unknown++;
                        var elapsed = Duration.ofNanos(System.nanoTime() - start);
                        if (unknown >= settings.unknownMaxAttempts() || elapsed.compareTo(settings.unknownMaxElapsed()) >= 0) {
                            return hold(record, e) ? Outcome.DONE : null;
                        }
                    }
                }
                if (!sleep(attempt)) {
                    return null;
                }
            }
        }
    }

    private boolean hold(ConsumerRecord<String, byte[]> record, Exception error) {
        var ref = record.topic() + "/" + record.partition() + "/" + record.offset();
        var errorClass = error.getClass().getSimpleName();
        for (int attempt = 1; ; attempt++) {
            try {
                var result = record.key() == null
                        ? AccountHolds.Result.UNATTRIBUTABLE
                        : switch (settings.holdTarget()) {
                    case ACCOUNT_ID -> holds.holdAccount(record.key(), errorClass, ref, settings.actor());
                    case PAYMENT_ID -> holds.holdPaymentAccount(record.key(), errorClass, ref, settings.actor());
                };
                switch (result) {
                    case HELD -> {
                        meters.counter("accounts_held_total", "service", settings.service(), "error_class", errorClass)
                                .increment();
                        LOG.log(ERROR, "account put on hold for " + ref + " key=" + record.key(), error);
                    }
                    case UNATTRIBUTABLE -> {
                        meters.counter("unattributable_records_total", "service", settings.service(), "topic",
                                record.topic()).increment();
                        LOG.log(WARNING, "unattributable record " + ref + " key=" + record.key() + " marked done", error);
                    }
                    case ALREADY_HELD -> {
                    }
                }
                return true;
            } catch (RuntimeException e) {
                if (!sleep(attempt)) {
                    return false;
                }
            }
        }
    }

    private boolean sleep(int attempt) {
        var cap = Math.min(settings.maxBackoff().toMillis(), settings.initialBackoff().toMillis() << Math.min(attempt - 1, 20));
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(cap / 2, cap + 1));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
