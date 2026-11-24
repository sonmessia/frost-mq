package io.frostmq.common;

public record PingReqPacket() implements MqttPacket {
  @Override
  public MqttPacketType type() {
    return MqttPacketType.PINGREQ;
  }
}
