package io.frostmq.client;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Supervises one clean MQTT session. It reconnects with capped exponential
 * backoff and invokes
 * the restorer after every CONNECT so callers can replay subscriptions and
 * retained state.
 */
public final class ReconnectingMqttSession implements AutoCloseable {
  public enum Status {
    CONNECTING, CONNECTED, RETRYING
  }

  public record State(Status status, Throwable cause) {
  }

  @FunctionalInterface
  public interface Restorer {
    void restore(MqttClient client) throws MqttException;
  }

  @FunctionalInterface
  public interface StateListener {
    void onState(State state);
  }

  private static final System.Logger LOG = System.getLogger(ReconnectingMqttSession.class.getName());
  private static final long INITIAL_RECONNECT_DELAY_MS = 250;
  private static final long MAX_RECONNECT_DELAY_MS = 10_000;

  private final MqttClient client;
  private final ConnectOptions options;
  private final Restorer restorer;
  private final StateListener states;
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicLong disconnectEpoch = new AtomicLong();
  private final CompletableFuture<Void> firstConnection = new CompletableFuture<>();
  private final Object signal = new Object();
  private volatile boolean connected;
  private boolean retryNow;
  private boolean started;
  private boolean closed;
  private volatile Thread supervisor;

  public ReconnectingMqttSession(MqttClient client, ConnectOptions options, Restorer restorer,
      StateListener states) {
    this.client = Objects.requireNonNull(client);
    this.options = Objects.requireNonNull(options);
    this.restorer = Objects.requireNonNull(restorer);
    this.states = Objects.requireNonNull(states);
  }

  /**
   * Starts asynchronous supervision and returns a stage completed after the first
   * restore.
   */
  public synchronized CompletionStage<Void> start() {
    if (started || closed)
      throw new IllegalStateException("MQTT session cannot be restarted");
    started = true;
    running.set(true);
    client.setConnectionListener(new ConnectionListener() {
      @Override
      public void onDisconnected(Throwable cause) {
        boolean wasConnected;
        synchronized (signal) {
          wasConnected = connected;
          disconnectEpoch.incrementAndGet();
          connected = false;
          signal.notifyAll();
        }
        if (running.get() && wasConnected)
          emit(Status.RETRYING, cause);
      }
    });
    supervisor = Thread.ofVirtual().name("frostmq-reconnect").start(this::supervise);
    return firstConnection.minimalCompletionStage();
  }

  /** Bypasses the current backoff delay. It has no effect while connected. */
  public void retryNow() {
    synchronized (signal) {
      if (!running.get() || connected)
        return;
      retryNow = true;
      signal.notifyAll();
    }
  }

  /**
   * Waits until the session has been fully restored; returns false after close.
   */
  public boolean awaitConnected() throws InterruptedException {
    synchronized (signal) {
      while (running.get() && !connected)
        signal.wait();
      return connected;
    }
  }

  private void supervise() {
    long delay = 0;
    while (running.get()) {
      try {
        if (!awaitRetry(delay))
          return;
        emit(Status.CONNECTING, null);
        connectAndRestore();
        delay = INITIAL_RECONNECT_DELAY_MS;
        emit(Status.CONNECTED, null);
        synchronized (signal) {
          while (running.get() && connected)
            signal.wait();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (MqttException e) {
        client.disconnect();
        if (!running.get())
          return;
        emit(Status.RETRYING, e);
        delay = delay == 0 ? INITIAL_RECONNECT_DELAY_MS
            : Math.min(delay * 2, MAX_RECONNECT_DELAY_MS);
      }
    }
  }

  private boolean awaitRetry(long delayMs) throws InterruptedException {
    if (delayMs == 0)
      return running.get();
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMs);
    synchronized (signal) {
      while (running.get() && !retryNow) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0)
          break;
        TimeUnit.NANOSECONDS.timedWait(signal, remaining);
      }
      retryNow = false;
      return running.get();
    }
  }

  private void connectAndRestore() throws MqttException {
    long epoch = disconnectEpoch.get();
    client.connect(options);
    restorer.restore(client);
    boolean valid;
    synchronized (signal) {
      valid = running.get() && disconnectEpoch.get() == epoch;
      if (valid) {
        connected = true;
        signal.notifyAll();
      }
    }
    if (!valid) {
      client.disconnect();
      throw new MqttException("Connection lost while restoring MQTT session");
    }
    firstConnection.complete(null);
  }

  private void emit(Status status, Throwable cause) {
    try {
      states.onState(new State(status, cause));
    } catch (RuntimeException e) {
      LOG.log(System.Logger.Level.WARNING, "MQTT session listener failed", e);
    }
  }

  @Override
  public synchronized void close() {
    if (closed)
      return;
    closed = true;
    running.set(false);
    synchronized (signal) {
      connected = false;
      signal.notifyAll();
    }
    Thread thread = supervisor;
    if (thread != null)
      thread.interrupt();
    client.disconnect();
    firstConnection.completeExceptionally(new CancellationException("MQTT session closed"));
    if (thread != null && thread != Thread.currentThread()) {
      try {
        thread.join(2_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
