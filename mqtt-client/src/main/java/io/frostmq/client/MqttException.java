package io.frostmq.client;

public class MqttException extends Exception {
    public MqttException(String message) { super(message); }
    public MqttException(String message, Throwable cause) { super(message, cause); }
}
