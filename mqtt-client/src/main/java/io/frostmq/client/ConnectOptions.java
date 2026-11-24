package io.frostmq.client;

import io.frostmq.common.PublishPacket;
import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public record ConnectOptions(String host, int port, String clientId, String username, byte[] password,
                             int keepAliveSeconds, PublishPacket willMessage, SSLContext sslContext) {
    public ConnectOptions {
        Objects.requireNonNull(host); Objects.requireNonNull(clientId);
        if (port < 1 || port > 65535 || keepAliveSeconds < 0 || keepAliveSeconds > 65535)
            throw new IllegalArgumentException("Invalid port or keep-alive");
        password = password == null ? null : password.clone();
        if (willMessage != null && (willMessage.qos() < 0 || willMessage.qos() > 1))
            throw new IllegalArgumentException("Only Will QoS 0 and 1 are supported");
    }
    public ConnectOptions(String host, int port, String clientId, String username, String password) {
        this(host, port, clientId, username, password == null ? null : password.getBytes(StandardCharsets.UTF_8),
                30, null, null);
    }
    public ConnectOptions(String host, int port, String clientId, String username, byte[] password,
                          int keepAliveSeconds, PublishPacket willMessage) {
        this(host, port, clientId, username, password, keepAliveSeconds, willMessage, null);
    }
    @Override public byte[] password() { return password == null ? null : password.clone(); }
}
