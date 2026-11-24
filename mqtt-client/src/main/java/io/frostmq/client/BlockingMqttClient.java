package io.frostmq.client;

import io.frostmq.common.*;
import javax.net.ssl.*;
import java.io.IOException;
import java.net.*;
import java.security.cert.*;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public final class BlockingMqttClient implements MqttClient {
    private static final System.Logger LOG = System.getLogger(BlockingMqttClient.class.getName());
    private record Request(MqttPacketType expected, CompletableFuture<MqttPacket> future) {}
    private static final class Connection {
        final Socket socket;
        final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        final ReentrantLock writeLock = new ReentrantLock();
        final Object subscriptionLock = new Object();
        final ConcurrentHashMap<String, MessageListener> listeners = new ConcurrentHashMap<>();
        final ConcurrentHashMap<Integer, Request> pending = new ConcurrentHashMap<>();
        final CompletableFuture<ConnackPacket> connack = new CompletableFuture<>();
        final AtomicBoolean closed = new AtomicBoolean();
        final int keepAlive;
        int nextId;
        volatile boolean accepted;
        volatile Thread reader;
        volatile long lastWrite = System.nanoTime();
        volatile long pingSent;
        Connection(int keepAlive, Socket socket) { this.keepAlive = keepAlive; this.socket = socket; }
        synchronized int reserve(Request request) throws MqttException {
            if (pending.size() == 65535) throw new MqttException("No packet identifiers available");
            do { nextId = nextId % 65535 + 1; } while (pending.containsKey(nextId));
            pending.put(nextId, request);
            return nextId;
        }
    }
    private final ReentrantLock lifecycle = new ReentrantLock();
    private volatile Connection connection;
    private volatile ConnectionListener connectionListener = new ConnectionListener() {};

    @Override public void setConnectionListener(ConnectionListener listener) {
        connectionListener = Objects.requireNonNull(listener);
    }

    @Override public void connect(ConnectOptions options) throws MqttException {
        Objects.requireNonNull(options);
        lifecycle.lock();
        try {
            if (connection != null && !connection.closed.get()) throw new MqttException("Already connected");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Socket socket;
            try {
                socket = options.sslContext() == null ? new Socket()
                        : options.sslContext().getSocketFactory().createSocket();
            } catch (IOException e) {
                throw new MqttException("Cannot create client socket", e);
            }
            Connection c = new Connection(options.keepAliveSeconds(), socket);
            connection = c;
            try {
                c.socket.connect(new InetSocketAddress(options.host(), options.port()), remainingMillis(deadline));
                c.socket.setTcpNoDelay(true);
                if (c.socket instanceof SSLSocket tlsSocket) {
                    SSLParameters parameters = tlsSocket.getSSLParameters();
                    parameters.setEndpointIdentificationAlgorithm("HTTPS");
                    tlsSocket.setSSLParameters(parameters);
                    c.socket.setSoTimeout(remainingMillis(deadline));
                    Future<?> handshake = c.workers.submit(() -> {
                        tlsSocket.startHandshake();
                        return null;
                    });
                    try { handshake.get(remainingNanos(deadline), TimeUnit.NANOSECONDS); }
                    finally { if (!handshake.isDone()) handshake.cancel(true); }
                    c.socket.setSoTimeout(0);
                    requireSubjectAlternativeName(tlsSocket, options.host());
                    remainingNanos(deadline);
                }
                c.workers.submit(() -> readLoop(c));
                send(c, new ConnectPacket("MQTT", 4, options.clientId(), true, options.keepAliveSeconds(),
                        options.username(), options.password(), options.willMessage()));
                ConnackPacket ack = c.connack.get(remainingNanos(deadline), TimeUnit.NANOSECONDS);
                if (ack.returnCode() != 0 || ack.sessionPresent()) throw new MqttException("CONNECT rejected, code " + ack.returnCode());
                if (c.closed.get()) throw new MqttException("Connection closed during CONNECT");
                if (c.keepAlive > 0) c.workers.submit(() -> heartbeat(c));
                try { connectionListener.onConnected(); }
                catch (RuntimeException e) { LOG.log(System.Logger.Level.WARNING, "Connection listener failed"); }
            } catch (Exception e) {
                terminate(c, e);
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                if (e instanceof MqttException mqtt) throw mqtt;
                throw new MqttException("CONNECT failed", e);
            }
        } finally { lifecycle.unlock(); }
    }

    private static long remainingNanos(long deadline) throws TimeoutException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) throw new TimeoutException("CONNECT timeout");
        return remaining;
    }

    private static int remainingMillis(long deadline) throws TimeoutException {
        long millis = TimeUnit.NANOSECONDS.toMillis(remainingNanos(deadline));
        return (int) Math.min(Integer.MAX_VALUE, Math.max(1, millis));
    }

    /** Endpoint identification verifies the match; this additionally forbids legacy CN fallback. */
    private static void requireSubjectAlternativeName(SSLSocket socket, String host) throws SSLHandshakeException {
        try {
            if (!(socket.getSession().getPeerCertificates()[0] instanceof X509Certificate certificate))
                throw new SSLHandshakeException("Broker did not present an X.509 certificate");
            Collection<List<?>> names = certificate.getSubjectAlternativeNames();
            int requiredType = ipLiteral(host) ? 7 : 2;
            boolean present = names != null && names.stream().anyMatch(name -> name.size() >= 2
                    && name.getFirst() instanceof Integer type && type == requiredType);
            if (!present) throw new SSLHandshakeException(requiredType == 7
                    ? "Broker certificate lacks an IP address SAN"
                    : "Broker certificate lacks a DNS SAN");
        } catch (SSLPeerUnverifiedException | CertificateParsingException e) {
            var failure = new SSLHandshakeException("Cannot verify broker certificate SAN");
            failure.initCause(e);
            throw failure;
        }
    }

    private static boolean ipLiteral(String host) {
        String value = host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
        return value.indexOf(':') >= 0 || value.matches("[0-9.]+");
    }

    @Override public void publish(String topic, byte[] payload, int qos, boolean retain) throws MqttException {
        if (!TopicMatcher.validTopic(topic) || payload == null || qos < 0 || qos > 1)
            throw new MqttException("Invalid publish topic, payload, or QoS");
        int remainingLength = 2 + topic.getBytes(java.nio.charset.StandardCharsets.UTF_8).length + (qos == 1 ? 2 : 0) + payload.length;
        if (remainingLength > PacketCodec.MAX_PACKET_SIZE) throw new MqttException("Packet exceeds 256 KB");
        Connection c = requireConnection(qos == 1);
        if (qos == 0) { send(c, new PublishPacket(topic, payload, 0, retain, false, 0)); return; }
        var request = new Request(MqttPacketType.PUBACK, new CompletableFuture<>());
        int id = c.reserve(request);
        try {
            send(c, new PublishPacket(topic, payload, 1, retain, false, id));
            await(c, request);
        } finally { c.pending.remove(id, request); }
    }

    @Override public int subscribe(String filter, int qos, MessageListener listener) throws MqttException {
        if (!TopicMatcher.validFilter(filter) || qos < 0 || qos > 2 || listener == null)
            throw new MqttException("Invalid subscription");
        Connection c = requireConnection(true);
        synchronized (c.subscriptionLock) {
            MessageListener previous = c.listeners.put(filter, listener);
            var request = new Request(MqttPacketType.SUBACK, new CompletableFuture<>());
            int id = c.reserve(request);
            boolean granted = false;
            try {
                send(c, new SubscribePacket(id, List.of(new SubscribePacket.Subscription(filter, qos))));
                var ack = (SubackPacket) await(c, request);
                if (ack.returnCodes().size() != 1 || ack.returnCodes().getFirst() == 2) throw new MqttException("Invalid SUBACK");
                int code = ack.returnCodes().getFirst();
                granted = code != 128;
                return code;
            } finally {
                c.pending.remove(id, request);
                if (!granted) {
                    if (previous == null) c.listeners.remove(filter, listener);
                    else c.listeners.put(filter, previous);
                }
            }
        }
    }

    @Override public void unsubscribe(String filter) throws MqttException {
        if (!TopicMatcher.validFilter(filter)) throw new MqttException("Invalid filter");
        Connection c = requireConnection(true);
        synchronized (c.subscriptionLock) {
            var request = new Request(MqttPacketType.UNSUBACK, new CompletableFuture<>());
            int id = c.reserve(request);
            try {
                send(c, new UnsubscribePacket(id, List.of(filter)));
                await(c, request);
                c.listeners.remove(filter);
            } finally { c.pending.remove(id, request); }
        }
    }

    private Connection requireConnection(boolean blocking) throws MqttException {
        Connection c = connection;
        if (c == null || !c.accepted || c.closed.get()) throw new MqttException("Not connected");
        if (blocking && Thread.currentThread() == c.reader)
            throw new MqttException("Blocking MQTT operations cannot run inside a message callback");
        return c;
    }

    private void send(Connection c, MqttPacket packet) throws MqttException {
        c.writeLock.lock();
        try {
            if (c.closed.get()) throw new MqttException("Connection closed");
            PacketCodec.encode(packet, c.socket.getOutputStream());
            c.lastWrite = System.nanoTime();
        } catch (IOException e) {
            terminate(c, e);
            throw new MqttException("MQTT send failed", e);
        } finally { c.writeLock.unlock(); }
    }

    private MqttPacket await(Connection c, Request request) throws MqttException {
        try { return request.future.get(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            terminate(c, e);
            throw new MqttException("MQTT operation interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            terminate(c, e);
            throw new MqttException("MQTT acknowledgement failed or timed out", e);
        }
    }

    private void readLoop(Connection c) {
        c.reader = Thread.currentThread();
        try {
            while (!c.closed.get()) {
                MqttPacket packet = PacketCodec.decode(c.socket.getInputStream());
                if (packet == null) throw new IOException("Broker closed connection");
                if (!c.connack.isDone()) {
                    if (!(packet instanceof ConnackPacket ack)) throw new IOException("Expected CONNACK");
                    c.accepted = ack.returnCode() == 0 && !ack.sessionPresent();
                    c.connack.complete(ack);
                    if (!c.accepted) return;
                    continue;
                }
                switch (packet) {
                    case PublishPacket p -> {
                        if (p.qos() > 1) throw new IOException("QoS 2 is unsupported");
                        if (p.qos() == 1) send(c, new PubackPacket(p.packetId()));
                        c.listeners.forEach((filter, listener) -> {
                            if (TopicMatcher.match(filter, p.topic())) {
                                try { listener.onMessage(p.topic(), p.payload(), p.qos(), p.retain()); }
                                catch (RuntimeException e) { LOG.log(System.Logger.Level.WARNING, "Message listener failed"); }
                            }
                        });
                    }
                    case PubackPacket p -> complete(c, p.packetId(), p);
                    case SubackPacket p -> complete(c, p.packetId(), p);
                    case UnsubackPacket p -> complete(c, p.packetId(), p);
                    case PingRespPacket ignored -> c.pingSent = 0;
                    default -> throw new IOException("Unexpected broker packet");
                }
            }
        } catch (Exception e) { terminate(c, e); }
        finally { terminate(c, new IOException("MQTT reader stopped")); }
    }

    private void complete(Connection c, int id, MqttPacket packet) throws IOException {
        Request request = c.pending.get(id);
        if (request != null) {
            if (request.expected != packet.type()) throw new IOException("Acknowledgement type mismatch");
            request.future.complete(packet);
        }
    }

    private void heartbeat(Connection c) {
        long interval = TimeUnit.MILLISECONDS.toNanos(Math.max(250, c.keepAlive * 500L));
        long pingTimeout = Math.max(TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(c.keepAlive));
        try {
            while (!c.closed.get()) {
                Thread.sleep(Math.max(100, Math.min(1000, c.keepAlive * 250L)));
                long now = System.nanoTime();
                if (c.pingSent != 0 && now - c.pingSent >= pingTimeout) {
                    terminate(c, new IOException("PINGRESP timeout")); return;
                }
                if (c.pingSent == 0 && now - c.lastWrite >= interval) {
                    c.pingSent = now;
                    send(c, new PingReqPacket());
                }
            }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        catch (MqttException e) { terminate(c, e); }
    }

    private void terminate(Connection c, Throwable cause) {
        if (!c.closed.compareAndSet(false, true)) return;
        c.accepted = false;
        try { c.socket.close(); } catch (IOException ignored) {}
        c.connack.completeExceptionally(cause == null ? new IOException("Disconnected") : cause);
        c.pending.values().forEach(request -> request.future.completeExceptionally(cause == null ? new IOException("Disconnected") : cause));
        c.pending.clear();
        c.listeners.clear();
        c.workers.shutdownNow();
        try { connectionListener.onDisconnected(cause); }
        catch (RuntimeException e) { LOG.log(System.Logger.Level.WARNING, "Connection listener failed"); }
    }

    @Override public void disconnect() {
        lifecycle.lock();
        try {
            Connection c = connection;
            if (c == null || c.closed.get()) return;
            try { if (c.accepted) send(c, new DisconnectPacket()); }
            catch (MqttException ignored) {}
            terminate(c, null);
        } finally { lifecycle.unlock(); }
    }
}
