package io.frostmq.common;

import java.util.List;

public record UnsubscribePacket(int packetId, List<String> filters) implements MqttPacket {
    public UnsubscribePacket { filters = List.copyOf(filters); }
    @Override public MqttPacketType type() { return MqttPacketType.UNSUBSCRIBE; }
}
