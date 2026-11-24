package io.frostmq.broker;

import io.frostmq.common.*;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class ClientSession {
    public static final int QUEUE_CAPACITY = 1000;
    static final long RETRY_NANOS = TimeUnit.SECONDS.toNanos(5);
    static final class InFlight {
        final PublishPacket packet;
        volatile PublishPacket retryPacket;
        volatile long lastSentNanos;
        InFlight(PublishPacket packet) { this.packet = packet; }
    }

    final BrokerServer broker;
    final Socket socket;
    final BlockingQueue<MqttPacket> outboundQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    final ConcurrentHashMap<Integer, InFlight> inFlightQos1Map = new ConcurrentHashMap<>();
    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final AtomicBoolean willCancelled = new AtomicBoolean();
    private final LongAdder dropped = new LongAdder();
    private int nextPacketId;
    volatile String clientId;
    volatile String username;
    volatile boolean cleanSession;
    volatile PublishPacket willMessage;
    volatile AuthService.Principal principal;
    volatile boolean connected;
    volatile int keepAliveSeconds;
    volatile long lastPacketNanos = System.nanoTime();
    volatile boolean closeAfterConnack;
    volatile Thread reader;
    volatile Thread writer;
    int consecutiveViolations;

    ClientSession(BrokerServer broker, Socket socket) { this.broker = broker; this.socket = socket; }
    public String clientId() { return clientId; }
    public String username() { return username; }
    public boolean isClosed() { return isClosed.get(); }
    public long droppedCount() { return dropped.sum(); }
    public int queuedCount() { return outboundQueue.size(); }
    public int inFlightCount() { return inFlightQos1Map.size(); }

    public boolean enqueue(MqttPacket packet) {
        if (isClosed()) return false;
        if (outboundQueue.offer(packet)) return true;
        if (packet instanceof PublishPacket publish && publish.qos() == 0) {
            dropped.increment();
            broker.recordOutboundDrop();
        } else closeSession("Outbound queue full");
        return false;
    }

    void deliver(PublishPacket source, int grantedQos, boolean retained) {
        int qos = Math.min(source.qos(), grantedQos);
        PublishPacket packet = null;
        synchronized (this) {
            if (isClosed()) return;
            if (qos == 0 || inFlightQos1Map.size() < QUEUE_CAPACITY) {
                int id = 0;
                if (qos == 1) {
                    do { nextPacketId = nextPacketId % 65535 + 1; } while (inFlightQos1Map.containsKey(nextPacketId));
                    id = nextPacketId;
                }
                packet = new PublishPacket(source.topic(), source.payload(), qos, retained, false, id);
                if (qos == 1) inFlightQos1Map.put(id, new InFlight(packet));
            }
        }
        // Closing can route a Will to another session. Never do that while holding this session's monitor.
        if (packet == null) closeSession("Too many unacknowledged QoS 1 messages");
        else enqueue(packet);
    }

    void acknowledge(int id) { inFlightQos1Map.remove(id); }

    void maintain(long now) {
        if (isClosed()) return;
        long timeout = connected ? (keepAliveSeconds == 0 ? Long.MAX_VALUE : TimeUnit.MILLISECONDS.toNanos(keepAliveSeconds * 1500L))
                : TimeUnit.SECONDS.toNanos(10);
        if (now - lastPacketNanos >= timeout) {
            closeSession(connected ? "Keep-alive exceeded" : "CONNECT timeout");
            return;
        }
        inFlightQos1Map.values().forEach(pending -> {
            if (pending.lastSentNanos != 0 && now - pending.lastSentNanos >= RETRY_NANOS) {
                pending.lastSentNanos = 0;
                PublishPacket p = pending.packet;
                if (pending.retryPacket == null)
                    pending.retryPacket = new PublishPacket(p.topic(), p.payload(), 1, p.retain(), true, p.packetId());
                enqueue(pending.retryPacket);
            }
        });
    }

    void writeLoop() {
        writer = Thread.currentThread();
        try {
            while (!isClosed()) {
                MqttPacket packet = outboundQueue.take();
                InFlight delivery = null;
                if (packet instanceof PublishPacket p && p.qos() == 1) {
                    delivery = inFlightQos1Map.get(p.packetId());
                    if (delivery == null || (delivery.packet != p && delivery.retryPacket != p)) continue;
                }
                PacketCodec.encode(packet, socket.getOutputStream());
                if (packet instanceof PublishPacket p && p.qos() == 1) {
                    if (inFlightQos1Map.get(p.packetId()) == delivery) delivery.lastSentNanos = System.nanoTime();
                }
                if (closeAfterConnack && packet instanceof ConnackPacket) {
                    closeSession("CONNECT rejected", true);
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            closeSession("Socket write failed");
        } finally { closeSession("Writer stopped"); }
    }

    void reject(int returnCode) {
        closeAfterConnack = true;
        enqueue(new ConnackPacket(false, returnCode));
    }

    public void closeSession(String reason) { closeSession(reason, false); }

    void closeSession(String reason, boolean suppressWill) {
        synchronized (this) {
            if (suppressWill) willCancelled.set(true);
            if (!isClosed.compareAndSet(false, true)) return;
        }
        try { socket.close(); } catch (IOException ignored) {}
        if (reader != null && reader != Thread.currentThread()) reader.interrupt();
        if (writer != null && writer != Thread.currentThread()) writer.interrupt();
        broker.remove(this);
        outboundQueue.clear();
        synchronized (this) { inFlightQos1Map.clear(); }
        if (connected && !willCancelled.get() && willMessage != null) broker.route(willMessage);
    }
}
