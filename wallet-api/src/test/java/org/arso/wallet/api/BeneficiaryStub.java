package org.arso.wallet.api;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/** In-process beneficiary service: any id starting with "b-" exists, "missing" does not, and it can be taken down. */
final class BeneficiaryStub {

    static volatile boolean down;

    private static final HttpServer SERVER = start();

    private BeneficiaryStub() {}

    static URI url() {
        return URI.create("http://localhost:" + SERVER.getAddress().getPort());
    }

    private static HttpServer start() {
        try {
            var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/beneficiaries/", exchange -> {
                var id = exchange.getRequestURI().getPath().substring("/beneficiaries/".length());
                var status = down ? 500 : id.startsWith("b-") ? 200 : 404;
                var body = status != 200 ? "" : """
                        {"beneficiary_id":"%s","name":"Jane Doe","account_masked":"****9876",\
                        "account_fingerprint":"hmac:abc","country":"DE"}""".formatted(id);
                var bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
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
