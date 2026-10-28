package org.arso.wallet.events;

public final class Events {

    public static final int SCHEMA_VERSION = 1;

    public static final String LEDGER_EVENTS = "ledger-events";
    public static final String WITHDRAW_REQUESTS = "withdraw-requests";
    public static final String VERIFICATION_RESULTS = "verification-results";
    public static final String TRANSFER_REQUESTS = "transfer-requests";

    public static final String CONTENT_TYPE_HEADER = "content-type";
    public static final String EVENT_TYPE_HEADER = "event_type";
    public static final String JSON = "application/json";

    public static final String WITHDRAW = "withdraw";
    public static final String ACCOUNT_HELD = "account_held";
    public static final String ACCOUNT_RELEASED = "account_released";

    private Events() {
    }
}
