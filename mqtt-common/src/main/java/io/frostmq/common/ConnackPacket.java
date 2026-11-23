package io.frostmq.common;

public record ConnackPacket(boolean sessionPresent, int returnCode) implements MqttPacket {
    @Override public MqttPacketType type() { return MqttPacketType.CONNACK; }
}
