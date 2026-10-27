package org.arso.wallet.api.application;

/** {@code error} is the error code of an error built in this request, null otherwise (also for replays). */
public record ApiResponse(int status, String body, String error) {

    public ApiResponse(int status, String body) {
        this(status, body, null);
    }
}
