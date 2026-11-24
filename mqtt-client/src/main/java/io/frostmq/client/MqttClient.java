package io.frostmq.client;

public interface MqttClient extends AutoCloseable {
  void connect(ConnectOptions options) throws MqttException;

  void publish(String topic, byte[] payload, int qos, boolean retain) throws MqttException;

  int subscribe(String filter, int qos, MessageListener listener) throws MqttException;

  void unsubscribe(String filter) throws MqttException;

  void setConnectionListener(ConnectionListener listener);

  void disconnect();

  @Override
  default void close() {
    disconnect();
  }
}
