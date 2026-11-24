package io.frostmq.client;

public interface ConnectionListener {
  default void onConnected() {
  }

  default void onDisconnected(Throwable cause) {
  }
}
