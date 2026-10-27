package org.arso.wallet.api.adapter.rest;

import static org.arso.wallet.ledger.domain.DomainException.Code.INVALID_REQUEST;

import io.micrometer.common.KeyValue;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.filter.ServerHttpObservationFilter;
import org.springframework.web.servlet.HandlerMapping;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

@RestController
public class WalletController {

    private static final Logger FAILED_REQUESTS = LoggerFactory.getLogger("wallet.failed-requests");
    private static final Logger LOG = LoggerFactory.getLogger(WalletController.class);
    private static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    private final WalletCommands commands;
    private final WalletQueries queries;
    private final Idempotency idempotency;
    private final ApiErrors errors;
    private final JsonMapper mapper;
    private final JsonMapper canonical;
    private final MeterRegistry meters;

    public WalletController(
            WalletCommands commands,
            WalletQueries queries,
            Idempotency idempotency,
            ApiErrors errors,
            JsonMapper mapper,
            MeterRegistry meters) {
        this.commands = commands;
        this.queries = queries;
        this.idempotency = idempotency;
        this.errors = errors;
        this.mapper = mapper;
        this.meters = meters;
        this.canonical = mapper.rebuild().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS).build();
    }

    @PostMapping("/accounts")
    ResponseEntity<String> createAccount(HttpServletRequest req) throws Exception {
        return post(req, Bodies.CreateAccount.class, (idem, b) -> commands.createAccount(idem, b.toCurrency()));
    }

    @PostMapping("/accounts/{id}/deposits")
    ResponseEntity<String> deposit(HttpServletRequest req, @PathVariable String id) throws Exception {
        return post(req, Bodies.Deposit.class, (idem, b) -> commands.deposit(
                idem, new AccountId(id), Bodies.money(b.amount(), b.currency()), b.externalTxId(), b.toPayer()));
    }

    @PostMapping("/accounts/{id}/withdrawals")
    ResponseEntity<String> withdraw(HttpServletRequest req, @PathVariable String id) throws Exception {
        return post(req, Bodies.Withdraw.class, (idem, b) -> commands.withdraw(
                idem, new AccountId(id), Bodies.money(b.amount(), b.currency()), b.requireBeneficiary()));
    }

    @PostMapping("/payments/{id}/cancel")
    ResponseEntity<String> cancel(HttpServletRequest req, @PathVariable String id) throws Exception {
        return post(req, Bodies.Reason.class, (idem, _) -> commands.cancel(idem, new PaymentId(id)));
    }

    @PostMapping("/internal/accounts/{id}/block")
    ResponseEntity<String> block(HttpServletRequest req, @PathVariable String id) throws Exception {
        return post(req, Bodies.Reason.class, (idem, b) -> commands.block(idem, new AccountId(id), b.reason()));
    }

    @PostMapping("/internal/accounts/{id}/unblock")
    ResponseEntity<String> unblock(HttpServletRequest req, @PathVariable String id) throws Exception {
        return post(req, Bodies.Reason.class, (idem, b) -> commands.unblock(idem, new AccountId(id), b.reason()));
    }

    @PostMapping("/internal/accounts/{id}/release")
    ResponseEntity<String> release(HttpServletRequest req, @PathVariable String id) throws Exception {
        return post(req, Bodies.Release.class, (idem, b) -> commands.release(
                idem, new AccountId(id), b.target(), Boolean.TRUE.equals(b.confirm()), b.reason()));
    }

    @GetMapping("/accounts/{id}")
    ResponseEntity<String> account(@PathVariable String id) {
        return ok(queries.account(new AccountId(id)));
    }

    @GetMapping("/accounts/{id}/ledger")
    ResponseEntity<String> ledger(
            @PathVariable String id,
            @RequestParam(name = "before_seq", required = false) String beforeSeq,
            @RequestParam(required = false) String limit) {
        var size = number("limit", limit);
        return ok(queries.ledger(new AccountId(id), number("before_seq", beforeSeq), size == null ? 50 : size.intValue()));
    }

    @GetMapping("/payments/{id}")
    ResponseEntity<String> payment(@PathVariable String id) {
        return ok(queries.payment(new PaymentId(id)));
    }

    @ExceptionHandler(RuntimeException.class)
    ResponseEntity<String> error(RuntimeException e, HttpServletRequest req) {
        var known = errors.of(e);
        if (known.isEmpty()) {
            LOG.error("{} {} failed", req.getMethod(), req.getRequestURI(), e);
        }
        return send(req, known.orElseGet(() -> errors.response(HttpStatus.INTERNAL_SERVER_ERROR.value(), "internal_error", "unexpected error")));
    }

    private <T> ResponseEntity<String> post(HttpServletRequest req, Class<T> type, BiFunction<Idem, T, ApiResponse> operation)
            throws IOException, NoSuchAlgorithmException {
        var key = req.getHeader(IDEMPOTENCY_KEY);
        if (key == null || key.isBlank() || key.length() > 128) {
            return send(req, errors.response(HttpStatus.BAD_REQUEST.value(), "invalid_request", "Idempotency-Key header is required"));
        }
        var body = req.getInputStream().readAllBytes();
        var idem = new Idem(key, hash(req, body));
        return send(req, idempotency.execute(idem, () -> operation.apply(idem, parse(body, type))));
    }

    private String hash(HttpServletRequest req, byte[] body) throws NoSuchAlgorithmException {
        var request = req.getMethod() + " " + req.getRequestURI() + "\n" + canonicalJson(body);
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

    private ResponseEntity<String> ok(Object view) {
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(mapper.writeValueAsString(view));
    }

    private ResponseEntity<String> send(HttpServletRequest req, ApiResponse response) {
        if (response.error() != null) {
            var route = req.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
            meters.counter("api_errors_total", "route", route == null ? "unknown" : route.toString(), "error", response.error())
                    .increment();
        }
        if (response.body() != null && response.body().contains("\"payment_id\"")) {
            ServerHttpObservationFilter.findObservationContext(req).ifPresent(context -> context
                    .addHighCardinalityKeyValue(KeyValue.of("payment.id", mapper.readTree(response.body()).path("payment_id").asString())));
        }
        if (HttpStatusCode.valueOf(response.status()).isError()) {
            FAILED_REQUESTS.info("{} {} {} {}", req.getMethod(), req.getRequestURI(), response.status(), response.body());
        }
        return ResponseEntity.status(response.status()).contentType(MediaType.APPLICATION_JSON).body(response.body());
    }

    private static Long number(String name, String value) {
        try {
            return value == null ? null : Long.valueOf(value);
        } catch (NumberFormatException e) {
            throw new DomainException(INVALID_REQUEST, name + " must be a number");
        }
    }
}
