package io.frostmq.common;

public record DisconnectPacket() implements MqttPacket {
    @Override public MqttPacketType type() { return MqttPacketType.DISCONNECT; }
}
