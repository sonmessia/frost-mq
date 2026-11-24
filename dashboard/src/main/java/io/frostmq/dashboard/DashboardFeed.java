package io.frostmq.dashboard;

import io.frostmq.client.*;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;

/** Owns the dashboard's clean MQTT session, subscription replay, and reconnect lifecycle. */
final class DashboardFeed implements AutoCloseable {
    enum Status { CONNECTING, CONNECTED, RETRYING }
    record State(Status status, Throwable cause) {}

    @FunctionalInterface
    interface StateListener { void onState(State state); }

    private static final System.Logger LOG = System.getLogger(DashboardFeed.class.getName());
    private static final long INITIAL_RECONNECT_DELAY_MS = 250;
    private static final long MAX_RECONNECT_DELAY_MS = 10_000;

    private final MqttClient client;
    private final ConnectOptions options;
    private final MessageListener messages;
    private final StateListener states;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong disconnectEpoch = new AtomicLong();
    private final Object signal = new Object();
    private volatile boolean connected;
    private boolean retryNow;
    private boolean started;
    private boolean closed;
    private volatile Thread supervisor;

    DashboardFeed(MqttClient client, ConnectOptions options, MessageListener messages, StateListener states) {
        this.client = Objects.requireNonNull(client);
        this.options = Objects.requireNonNull(options);
        this.messages = Objects.requireNonNull(messages);
        this.states = Objects.requireNonNull(states);
    }

    synchronized void start() {
        if (started || closed) throw new IllegalStateException("Dashboard feed cannot be restarted");
        started = true;
        running.set(true);
        client.setConnectionListener(new ConnectionListener() {
            @Override public void onDisconnected(Throwable cause) {
                boolean wasConnected;
                synchronized (signal) {
                    wasConnected = connected;
                    disconnectEpoch.incrementAndGet();
                    connected = false;
                    signal.notifyAll();
                }
                if (running.get() && wasConnected) emit(Status.RETRYING, cause);
            }
        });
        supervisor = Thread.ofVirtual().name("frostmq-dashboard-feed").start(this::supervise);
    }

    void retryNow() {
        synchronized (signal) {
            retryNow = true;
            signal.notifyAll();
        }
    }

    private void supervise() {
        long delay = 0;
        while (running.get()) {
            try {
                if (!awaitRetry(delay)) return;
                emit(Status.CONNECTING, null);
                connectAndSubscribe();
                delay = INITIAL_RECONNECT_DELAY_MS;
                emit(Status.CONNECTED, null);
                synchronized (signal) {
                    while (running.get() && connected) signal.wait();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (MqttException e) {
                client.disconnect();
                if (!running.get()) return;
                emit(Status.RETRYING, e);
                delay = delay == 0 ? INITIAL_RECONNECT_DELAY_MS
                        : Math.min(delay * 2, MAX_RECONNECT_DELAY_MS);
            }
        }
    }

    private boolean awaitRetry(long delayMs) throws InterruptedException {
        if (delayMs == 0) return running.get();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMs);
        synchronized (signal) {
            while (running.get() && !retryNow) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) break;
                TimeUnit.NANOSECONDS.timedWait(signal, remaining);
            }
            retryNow = false;
            return running.get();
        }
    }

    private void connectAndSubscribe() throws MqttException {
        long epoch = disconnectEpoch.get();
        client.connect(options);
        if (client.subscribe("coldchain/#", 1, messages) == 128) {
            client.disconnect();
            throw new MqttException("Viewer subscription denied");
        }
        boolean valid;
        synchronized (signal) {
            valid = running.get() && disconnectEpoch.get() == epoch;
            if (valid) connected = true;
        }
        if (!valid) {
            client.disconnect();
            throw new MqttException("Connection lost while replaying dashboard subscription");
        }
    }

    private void emit(Status status, Throwable cause) {
        try { states.onState(new State(status, cause)); }
        catch (RuntimeException e) { LOG.log(System.Logger.Level.WARNING, "Dashboard state listener failed", e); }
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        running.set(false);
        synchronized (signal) {
            connected = false;
            signal.notifyAll();
        }
        Thread thread = supervisor;
        if (thread != null) thread.interrupt();
        client.disconnect();
        if (thread != null && thread != Thread.currentThread()) {
            try { thread.join(2_000); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
}
