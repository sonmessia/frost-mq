package io.frostmq.common;

public record PingRespPacket() implements MqttPacket {
  @Override
  public MqttPacketType type() {
    return MqttPacketType.PINGRESP;
  }
}
