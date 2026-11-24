package io.frostmq.common;

import java.util.List;

public record SubackPacket(int packetId, List<Integer> returnCodes) implements MqttPacket {
  public SubackPacket {
    returnCodes = List.copyOf(returnCodes);
  }

  @Override
  public MqttPacketType type() {
    return MqttPacketType.SUBACK;
  }
}
