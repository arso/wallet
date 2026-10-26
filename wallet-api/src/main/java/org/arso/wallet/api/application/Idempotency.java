package org.arso.wallet.api.application;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

import java.time.Clock;
import java.util.function.Supplier;
import org.arso.wallet.ledger.port.ConditionFailedException;
import org.arso.wallet.ledger.port.Conflict.ResponseExists;
import org.arso.wallet.ledger.port.LedgerStore;
import org.arso.wallet.ledger.port.StoredResponse;

/** Request flow for every POST (plan 3.8). */
public final class Idempotency {

    private final LedgerStore store;
    private final ApiErrors errors;
    private final Clock clock;

    public Idempotency(LedgerStore store, ApiErrors errors, Clock clock) {
        this.store = store;
        this.errors = errors;
        this.clock = clock;
    }

    public ApiResponse execute(Idem idem, Supplier<ApiResponse> operation) {
        var stored = store.findResponse(idem.key(), clock.instant());
        if (stored.isPresent()) {
            return replay(idem, stored.get());
        }
        try {
            return operation.get();
        } catch (RuntimeException e) {
            if (e instanceof ConditionFailedException c && c.conflict() instanceof ResponseExists(var winner)) {
                return replay(idem, winner);
            }
            var again = store.findResponse(idem.key(), clock.instant());
            if (again.isPresent()) {
                return replay(idem, again.get());
            }
            var error = errors.of(e).orElseThrow(() -> e);
            if (error.status() < HTTP_INTERNAL_ERROR) {
                store.storeResponse(idem.key(), idem.stored(error), clock.instant());
            }
            return error;
        }
    }

    private ApiResponse replay(Idem idem, StoredResponse stored) {
        return stored.requestHash().equals(idem.hash())
                ? new ApiResponse(stored.status(), stored.body())
                : errors.response(HTTP_CONFLICT, "idempotency_mismatch", "Idempotency-Key reused with a different request");
    }
}
