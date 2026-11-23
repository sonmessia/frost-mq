package io.frostmq.common;

import java.util.Objects;

public record ConnectPacket(String protocolName, int protocolLevel, String clientId,
                            boolean cleanSession, int keepAliveSeconds, String username,
                            byte[] password, PublishPacket willMessage) implements MqttPacket {
    public ConnectPacket {
        Objects.requireNonNull(protocolName);
        Objects.requireNonNull(clientId);
        password = password == null ? null : password.clone();
    }
    @Override public byte[] password() { return password == null ? null : password.clone(); }
    @Override public MqttPacketType type() { return MqttPacketType.CONNECT; }
}
