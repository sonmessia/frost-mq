package io.frostmq.client;

@FunctionalInterface
public interface MessageListener {
    /** Runs on the reader virtual thread. Hand off slow work; never call blocking MQTT methods here. */
    void onMessage(String topic, byte[] payload, int qos, boolean retained);
}
