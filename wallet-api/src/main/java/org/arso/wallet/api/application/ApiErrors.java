package org.arso.wallet.api.application;

import java.util.Optional;
import org.arso.wallet.api.application.Views.ErrorView;
import org.arso.wallet.api.port.BeneficiaryUnavailableException;
import org.arso.wallet.api.port.JsonWriter;
import org.arso.wallet.ledger.application.LockRetriesExhaustedException;
import org.arso.wallet.ledger.domain.DomainException;
import org.arso.wallet.ledger.port.TransientStoreException;

public final class ApiErrors {

    private final JsonWriter json;

    public ApiErrors(JsonWriter json) {
        this.json = json;
    }

    /** Known failures as responses (spec 10). Empty for bugs and infrastructure errors. */
    public Optional<ApiResponse> of(RuntimeException e) {
        return Optional.ofNullable(switch (e) {
            case DomainException d -> response(status(d.code()), d.code().name().toLowerCase(), d.getMessage());
            case LockRetriesExhaustedException _ -> response(503, "lock_retries_exhausted", "try again");
            case TransientStoreException _ -> response(503, "store_unavailable", "try again");
            case BeneficiaryUnavailableException _ -> response(503, "beneficiary_unavailable", "try again");
            default -> null;
        });
    }

    public ApiResponse response(int status, String error, String message) {
        return new ApiResponse(status, json.write(new ErrorView(error, message)));
    }

    private static int status(DomainException.Code code) {
        return switch (code) {
            case INVALID_REQUEST, INVALID_AMOUNT, INVALID_CURRENCY, CURRENCY_MISMATCH -> 400;
            case NOT_FOUND -> 404;
            case INSUFFICIENT_FUNDS -> 422;
            case ACCOUNT_BLOCKED, ACCOUNT_ON_HOLD, ACCOUNT_NOT_ON_HOLD, CONFIRM_REQUIRED, INVALID_TRANSITION,
                    NOT_CANCELLABLE, DUPLICATE_DEPOSIT -> 409;
        };
    }
}
