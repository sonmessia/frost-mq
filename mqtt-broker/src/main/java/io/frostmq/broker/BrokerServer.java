package io.frostmq.broker;

import io.frostmq.common.*;
import java.io.IOException;
import java.net.*;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public final class BrokerServer implements AutoCloseable {
    private final AuthService auth;
    private final String host;
    private final int requestedPort;
    private final SubscriptionTree subscriptions = new SubscriptionTree();
    private final RetainedStore retained = new RetainedStore();
    private final ConcurrentHashMap<String, ClientSession> sessions = new ConcurrentHashMap<>();
    private final Set<ClientSession> connections = ConcurrentHashMap.newKeySet();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService maintenance = Executors.newSingleThreadScheduledExecutor(
            task -> Thread.ofPlatform().daemon().name("frostmq-maintenance").unstarted(task));
    private final LongAdder dropped = new LongAdder();
    private volatile ServerSocket serverSocket;
    private volatile boolean running;
    private boolean closed;

    public BrokerServer(String host, int port, AuthService auth) {
        this.host = host; this.requestedPort = port; this.auth = auth;
    }
    public BrokerServer(int port, AuthService auth) { this("127.0.0.1", port, auth); }

    public synchronized void start() throws IOException {
        if (running || closed) throw new IllegalStateException("Broker already started or closed");
        var socket = new ServerSocket();
        try {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(host, requestedPort));
        } catch (IOException e) { socket.close(); throw e; }
        serverSocket = socket;
        running = true;
        workers.submit(this::acceptLoop);
        maintenance.scheduleAtFixedRate(() -> {
            long now = System.nanoTime();
            connections.forEach(session -> session.maintain(now));
        }, 100, 100, TimeUnit.MILLISECONDS);
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                var session = new ClientSession(this, socket);
                connections.add(session);
                if (!running) { session.closeSession("Broker stopping", true); continue; }
                workers.submit(session::writeLoop);
                workers.submit(new ConnectionHandler(session));
            } catch (IOException e) {
                if (running) System.getLogger(getClass().getName()).log(System.Logger.Level.ERROR, "Accept failed", e);
            } catch (RejectedExecutionException ignored) { return; }
        }
    }

    synchronized boolean register(ClientSession session) {
        ClientSession old;
        synchronized (session) {
            if (!running || session.isClosed()) return false;
            old = sessions.put(session.clientId, session);
            session.connected = true;
            session.lastPacketNanos = System.nanoTime();
        }
        if (old != null && old != session) old.closeSession("Client ID replaced", true);
        return session.enqueue(new ConnackPacket(false, 0));
    }

    void remove(ClientSession session) {
        connections.remove(session);
        if (session.clientId != null) sessions.remove(session.clientId, session);
        subscriptions.remove(session);
    }

    public void route(PublishPacket packet) {
        retained.update(packet);
        subscriptions.match(packet.topic()).forEach((session, qos) -> session.deliver(packet, qos, false));
    }

    AuthService auth() { return auth; }
    public SubscriptionTree subscriptions() { return subscriptions; }
    public RetainedStore retained() { return retained; }
    public int port() { return serverSocket == null ? requestedPort : serverSocket.getLocalPort(); }
    public int sessionCount() { return sessions.size(); }
    public int connectionCount() { return connections.size(); }
    public long droppedCount() { return dropped.sum(); }
    void recordDrop() { dropped.increment(); }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        running = false;
        if (serverSocket != null) try { serverSocket.close(); } catch (IOException ignored) {}
        connections.forEach(session -> session.closeSession("Broker shutdown", true));
        maintenance.shutdownNow();
        workers.shutdownNow();
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 1883;
        Path users = Path.of(args.length > 1 ? args[1] : "users.json");
        Path acl = Path.of(args.length > 2 ? args[2] : "acl.json");
        String host = args.length > 3 ? args[3] : "127.0.0.1";
        var broker = new BrokerServer(host, port, new AuthService(users, acl));
        Runtime.getRuntime().addShutdownHook(new Thread(broker::close));
        broker.start();
        System.out.println("FrostMQ listening on " + host + ":" + broker.port());
        new CountDownLatch(1).await();
    }
}
