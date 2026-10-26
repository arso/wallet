package org.arso.wallet.mocks;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.http.HttpStatus;

final class WalletStub {

    record Call(String path, String idempotencyKey) {}

    static final List<Call> CALLS = new CopyOnWriteArrayList<>();
    static final Set<String> ON_HOLD = ConcurrentHashMap.newKeySet();
    private static final HttpServer SERVER = start();

    private WalletStub() {}

    static URI url() {
        return URI.create("http://localhost:" + SERVER.getAddress().getPort());
    }

    static List<Call> calls(String accountId) {
        return CALLS.stream().filter(c -> c.path().contains(accountId)).toList();
    }

    private static HttpServer start() {
        try {
            var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/internal/accounts/", exchange -> {
                var path = exchange.getRequestURI().getPath();
                CALLS.add(new Call(path, exchange.getRequestHeaders().getFirst("Idempotency-Key")));
                var held = ON_HOLD.stream().anyMatch(path::contains);
                var body = held ? "{\"error\":\"account_on_hold\",\"message\":\"account on hold\"}" : "{}";
                var bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders((held ? HttpStatus.CONFLICT : HttpStatus.OK).value(), bytes.length);
                try (var out = exchange.getResponseBody()) {
                    out.write(bytes);
                }
            });
            server.start();
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
