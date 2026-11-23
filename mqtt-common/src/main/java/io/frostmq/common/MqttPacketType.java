package io.frostmq.common;

import java.io.IOException;

public enum MqttPacketType {
    CONNECT(1), CONNACK(2), PUBLISH(3), PUBACK(4), SUBSCRIBE(8), SUBACK(9),
    UNSUBSCRIBE(10), UNSUBACK(11), PINGREQ(12), PINGRESP(13), DISCONNECT(14);

    private final int code;
    MqttPacketType(int code) { this.code = code; }
    public int code() { return code; }

    public static MqttPacketType fromHeader(int header) throws IOException {
        int type = header >>> 4;
        for (var value : values()) if (value.code == type) return value;
        throw new IOException("Unsupported MQTT packet type: " + type);
    }
}
