package io.frostmq.common;

import java.util.List;

public record SubscribePacket(int packetId, List<Subscription> subscriptions) implements MqttPacket {
  public SubscribePacket {
    subscriptions = List.copyOf(subscriptions);
  }

  public record Subscription(String filter, int qos) {
  }

  @Override
  public MqttPacketType type() {
    return MqttPacketType.SUBSCRIBE;
  }
}
