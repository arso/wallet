package org.arso.wallet.api.adapter.rest;

import static org.arso.wallet.ledger.domain.DomainException.Code.INVALID_REQUEST;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.function.BiFunction;
import org.arso.wallet.api.application.ApiErrors;
import org.arso.wallet.api.application.ApiResponse;
import org.arso.wallet.api.application.Idem;
import org.arso.wallet.api.application.Idempotency;
import org.arso.wallet.api.application.WalletCommands;
import org.arso.wallet.api.application.WalletQueries;
import org.arso.wallet.ledger.domain.AccountId;
import org.arso.wallet.ledger.domain.DomainException;
import org.arso.wallet.ledger.domain.PaymentId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

public final class WalletHandlers {

    private static final Logger FAILED_REQUESTS = LoggerFactory.getLogger("wallet.failed-requests");
    private static final Logger LOG = LoggerFactory.getLogger(WalletHandlers.class);
    private static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    private final WalletCommands commands;
    private final WalletQueries queries;
    private final Idempotency idempotency;
    private final ApiErrors errors;
    private final JsonMapper mapper;
    private final JsonMapper canonical;

    public WalletHandlers(
            WalletCommands commands, WalletQueries queries, Idempotency idempotency, ApiErrors errors, JsonMapper mapper) {
        this.commands = commands;
        this.queries = queries;
        this.idempotency = idempotency;
        this.errors = errors;
        this.mapper = mapper;
        this.canonical = mapper.rebuild().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS).build();
    }

    public RouterFunction<ServerResponse> routes() {
        return RouterFunctions.route()
                .POST("/accounts", req -> post(req, Bodies.CreateAccount.class,
                        (idem, b) -> commands.createAccount(idem, b.toCurrency())))
                .POST("/accounts/{id}/deposits", req -> post(req, Bodies.Deposit.class,
                        (idem, b) -> commands.deposit(idem, accountId(req), Bodies.money(b.amount(), b.currency()),
                                b.externalTxId(), b.toPayer())))
                .POST("/accounts/{id}/withdrawals", req -> post(req, Bodies.Withdraw.class,
                        (idem, b) -> commands.withdraw(idem, accountId(req), Bodies.money(b.amount(), b.currency()),
                                b.requireBeneficiary())))
                .POST("/payments/{id}/cancel", req -> post(req, Bodies.Reason.class,
                        (idem, _) -> commands.cancel(idem, new PaymentId(req.pathVariable("id")))))
                .POST("/internal/accounts/{id}/block", req -> post(req, Bodies.Reason.class,
                        (idem, b) -> commands.block(idem, accountId(req), b.reason())))
                .POST("/internal/accounts/{id}/unblock", req -> post(req, Bodies.Reason.class,
                        (idem, b) -> commands.unblock(idem, accountId(req), b.reason())))
                .POST("/internal/accounts/{id}/release", req -> post(req, Bodies.Release.class,
                        (idem, b) -> commands.release(idem, accountId(req), b.target(),
                                Boolean.TRUE.equals(b.confirm()), b.reason())))
                .GET("/accounts/{id}", req -> ok(queries.account(accountId(req))))
                .GET("/accounts/{id}/ledger", req -> ok(queries.ledger(
                        accountId(req), longParam(req, "before_seq"), longParam(req, "limit", 50L).intValue())))
                .GET("/payments/{id}", req -> ok(queries.payment(new PaymentId(req.pathVariable("id")))))
                .onError(RuntimeException.class, this::error)
                .build();
    }

    private <T> ServerResponse post(ServerRequest req, Class<T> type, BiFunction<Idem, T, ApiResponse> operation)
            throws Exception {
        var key = req.headers().firstHeader(IDEMPOTENCY_KEY);
        if (key == null || key.isBlank() || key.length() > 128) {
            return send(req, errors.response(400, "invalid_request", "Idempotency-Key header is required"));
        }
        var body = req.body(byte[].class);
        var idem = new Idem(key, hash(req, body));
        return send(req, idempotency.execute(idem, () -> operation.apply(idem, parse(body, type))));
    }

    private String hash(ServerRequest req, byte[] body) throws NoSuchAlgorithmException {
        var request = req.method().name() + " " + req.path() + "\n" + canonicalJson(body);
        var digest = MessageDigest.getInstance("SHA-256").digest(request.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest);
    }

    private String canonicalJson(byte[] body) {
        if (body.length == 0) {
            return "";
        }
        try {
            return canonical.writeValueAsString(mapper.readValue(body, Object.class));
        } catch (JacksonException e) {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    private <T> T parse(byte[] body, Class<T> type) {
        try {
            return mapper.readValue(body.length == 0 ? "{}".getBytes() : body, type);
        } catch (JacksonException e) {
            throw new DomainException(INVALID_REQUEST, "malformed request body");
        }
    }

    private ServerResponse ok(Object view) {
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(mapper.writeValueAsString(view));
    }

    private ServerResponse error(Throwable e, ServerRequest req) {
        var known = errors.of((RuntimeException) e);
        if (known.isEmpty()) {
            LOG.error("{} {} failed", req.method(), req.path(), e);
        }
        return send(req, known.orElseGet(() -> errors.response(500, "internal_error", "unexpected error")));
    }

    private static ServerResponse send(ServerRequest req, ApiResponse response) {
        if (response.status() >= 400) {
            FAILED_REQUESTS.info("{} {} {} {}", req.method(), req.path(), response.status(), response.body());
        }
        return ServerResponse.status(response.status()).contentType(MediaType.APPLICATION_JSON).body(response.body());
    }

    private static AccountId accountId(ServerRequest req) {
        return new AccountId(req.pathVariable("id"));
    }

    private static Long longParam(ServerRequest req, String name) {
        return longParam(req, name, null);
    }

    private static Long longParam(ServerRequest req, String name, Long fallback) {
        try {
            return req.param(name).map(Long::valueOf).orElse(fallback);
        } catch (NumberFormatException e) {
            throw new DomainException(INVALID_REQUEST, name + " must be a number");
        }
    }
}
