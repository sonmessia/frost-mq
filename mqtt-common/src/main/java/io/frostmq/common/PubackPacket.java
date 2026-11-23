package io.frostmq.common;

public record PubackPacket(int packetId) implements MqttPacket {
    @Override public MqttPacketType type() { return MqttPacketType.PUBACK; }
}
