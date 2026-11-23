package io.frostmq.common;

public sealed interface MqttPacket permits ConnectPacket, ConnackPacket, PublishPacket, PubackPacket,
        SubscribePacket, SubackPacket, UnsubscribePacket, UnsubackPacket,
        PingReqPacket, PingRespPacket, DisconnectPacket {
    MqttPacketType type();
}
