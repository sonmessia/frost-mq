package io.frostmq.common;

import java.util.Objects;

public record PublishPacket(String topic, byte[] payload, int qos, boolean retain,
    boolean dup, int packetId) implements MqttPacket {
  public PublishPacket {
    Objects.requireNonNull(topic);
    payload = Objects.requireNonNull(payload).clone();
  }

  @Override
  public byte[] payload() {
    return payload.clone();
  }

  public int payloadLength() {
    return payload.length;
  }

  @Override
  public MqttPacketType type() {
    return MqttPacketType.PUBLISH;
  }
}
