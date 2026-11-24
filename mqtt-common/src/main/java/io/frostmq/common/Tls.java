package io.frostmq.common;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Objects;

/** PKCS12 TLS context loading shared by the broker and client. */
public final class Tls {
    private static final String KEYSTORE_ENV = "FROSTMQ_TLS_KEYSTORE";
    private static final String KEYSTORE_PASSWORD_ENV = "FROSTMQ_TLS_KEYSTORE_PASSWORD";
    private static final String TRUSTSTORE_ENV = "FROSTMQ_TLS_TRUSTSTORE";
    private static final String TRUSTSTORE_PASSWORD_ENV = "FROSTMQ_TLS_TRUSTSTORE_PASSWORD";

    private Tls() {}

    public static SSLContext server(Path keyStore, char[] password) throws IOException {
        Objects.requireNonNull(keyStore);
        Objects.requireNonNull(password);
        KeyStore store = load(keyStore, password);
        try {
            var keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keys.init(store, password);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys.getKeyManagers(), null, null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("Cannot initialize TLS server key material", e);
        }
    }

    public static SSLContext client(Path trustStore, char[] password) throws IOException {
        Objects.requireNonNull(trustStore);
        Objects.requireNonNull(password);
        KeyStore store = load(trustStore, password);
        try {
            var trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trust.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("Cannot initialize TLS client trust material", e);
        }
    }

    /** Returns null when TLS is not configured, preserving the plaintext demo default. */
    public static SSLContext serverFromEnvironment() throws IOException {
        String path = System.getenv(KEYSTORE_ENV);
        if (path == null || path.isBlank()) return null;
        return server(Path.of(path), requiredPassword(KEYSTORE_PASSWORD_ENV));
    }

    /** Returns null when TLS is not configured, preserving the plaintext demo default. */
    public static SSLContext clientFromEnvironment() throws IOException {
        String path = System.getenv(TRUSTSTORE_ENV);
        if (path == null || path.isBlank()) return null;
        return client(Path.of(path), requiredPassword(TRUSTSTORE_PASSWORD_ENV));
    }

    private static KeyStore load(Path path, char[] password) throws IOException {
        char[] copy = password.clone();
        try (InputStream input = Files.newInputStream(path)) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(input, copy);
            return store;
        } catch (GeneralSecurityException e) {
            throw new IOException("Cannot load PKCS12 keystore " + path, e);
        } finally {
            Arrays.fill(copy, '\0');
        }
    }

    private static char[] requiredPassword(String name) throws IOException {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) throw new IOException(name + " is required when TLS is enabled");
        return value.toCharArray();
    }
}
