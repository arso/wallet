package org.arso.wallet.api.application;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

import java.util.Optional;
import org.arso.wallet.api.application.Views.ProblemView;
import org.arso.wallet.api.port.BeneficiaryUnavailableException;
import org.arso.wallet.api.port.JsonWriter;
import org.arso.wallet.ledger.application.LockRetriesExhaustedException;
import org.arso.wallet.ledger.domain.DomainException;
import org.arso.wallet.ledger.port.TransientStoreException;

public final class ApiErrors {

    private static final int HTTP_UNPROCESSABLE_CONTENT = 422;

    private final JsonWriter json;

    public ApiErrors(JsonWriter json) {
        this.json = json;
    }

    /** Known failures as responses (spec 10). Empty for bugs and infrastructure errors. */
    public Optional<ApiResponse> of(RuntimeException e) {
        return Optional.ofNullable(switch (e) {
            case DomainException d -> response(status(d.code()), d.code().name().toLowerCase(), d.getMessage());
            case LockRetriesExhaustedException _ -> response(HTTP_UNAVAILABLE, "lock_retries_exhausted", "try again");
            case TransientStoreException _ -> response(HTTP_UNAVAILABLE, "store_unavailable", "try again");
            case BeneficiaryUnavailableException _ -> response(HTTP_UNAVAILABLE, "beneficiary_unavailable", "try again");
            default -> null;
        });
    }

    public ApiResponse response(int status, String error, String message) {
        return new ApiResponse(status, json.write(new ProblemView("about:blank", title(status), status, message, error)), error);
    }

    private static String title(int status) {
        return switch (status) {
            case HTTP_BAD_REQUEST -> "Bad Request";
            case HTTP_NOT_FOUND -> "Not Found";
            case HTTP_CONFLICT -> "Conflict";
            case HTTP_UNPROCESSABLE_CONTENT -> "Unprocessable Content";
            case HTTP_UNAVAILABLE -> "Service Unavailable";
            default -> "Internal Server Error";
        };
    }

    private static int status(DomainException.Code code) {
        return switch (code) {
            case INVALID_REQUEST, INVALID_AMOUNT, INVALID_CURRENCY, CURRENCY_MISMATCH -> HTTP_BAD_REQUEST;
            case NOT_FOUND -> HTTP_NOT_FOUND;
            case INSUFFICIENT_FUNDS -> HTTP_UNPROCESSABLE_CONTENT;
            case ACCOUNT_BLOCKED, ACCOUNT_ON_HOLD, ACCOUNT_NOT_ON_HOLD, CONFIRM_REQUIRED, INVALID_TRANSITION,
                    NOT_CANCELLABLE, DUPLICATE_DEPOSIT -> HTTP_CONFLICT;
        };
    }
}
