package org.arso.wallet.api.port;

@FunctionalInterface
public interface JsonWriter {

    String write(Object value);
}
