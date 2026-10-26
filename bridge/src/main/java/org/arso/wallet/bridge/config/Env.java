package org.arso.wallet.bridge.config;

final class Env {

    private Env() {}

    static String get(String name, String fallback) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
