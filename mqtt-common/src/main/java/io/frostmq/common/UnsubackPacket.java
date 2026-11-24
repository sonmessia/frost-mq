package io.frostmq.common;

public record UnsubackPacket(int packetId) implements MqttPacket {
  @Override
  public MqttPacketType type() {
    return MqttPacketType.UNSUBACK;
  }
}
