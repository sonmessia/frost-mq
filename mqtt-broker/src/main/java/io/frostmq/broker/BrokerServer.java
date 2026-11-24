package io.frostmq.broker;

import io.frostmq.common.*;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.*;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public final class BrokerServer implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(BrokerServer.class.getName());
    private final AuthService auth;
    private final String host;
    private final int requestedPort;
    private final SSLContext sslContext;
    private final BrokerLimits limits;
    private final SubscriptionTree subscriptions;
    private final RetainedStore retained;
    private final ConcurrentHashMap<String, ClientSession> sessions = new ConcurrentHashMap<>();
    private final Set<ClientSession> connections = ConcurrentHashMap.newKeySet();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService maintenance = Executors.newSingleThreadScheduledExecutor(
            task -> Thread.ofPlatform().daemon().name("frostmq-maintenance").unstarted(task));
    private final LongAdder dropped = new LongAdder();
    private final CountDownLatch closeComplete = new CountDownLatch(1);
    private volatile ServerSocket serverSocket;
    private volatile Thread acceptor;
    private volatile boolean running;
    private boolean closed;

    public BrokerServer(String host, int port, AuthService auth) {
        this(host, port, auth, null, BrokerLimits.DEFAULT);
    }
    public BrokerServer(int port, AuthService auth) { this("127.0.0.1", port, auth); }
    public BrokerServer(String host, int port, AuthService auth, SSLContext sslContext) {
        this(host, port, auth, sslContext, BrokerLimits.DEFAULT);
    }
    public BrokerServer(int port, AuthService auth, SSLContext sslContext) {
        this("127.0.0.1", port, auth, sslContext);
    }
    public BrokerServer(int port, AuthService auth, BrokerLimits limits) {
        this("127.0.0.1", port, auth, null, limits);
    }
    public BrokerServer(String host, int port, AuthService auth, SSLContext sslContext, BrokerLimits limits) {
        this.host = host;
        this.requestedPort = port;
        this.auth = auth;
        this.sslContext = sslContext;
        this.limits = Objects.requireNonNull(limits);
        subscriptions = new SubscriptionTree(limits);
        retained = new RetainedStore(limits.maxRetainedMessages());
    }

    public synchronized void start() throws IOException {
        if (running || closed) throw new IllegalStateException("Broker already started or closed");
        var socket = sslContext == null ? new ServerSocket() : sslContext.getServerSocketFactory().createServerSocket();
        try {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(host, requestedPort));
        } catch (IOException e) { socket.close(); throw e; }
        serverSocket = socket;
        running = true;
        acceptor = Thread.ofPlatform().daemon().name("frostmq-acceptor").start(this::acceptLoop);
        maintenance.scheduleAtFixedRate(() -> {
            long now = System.nanoTime();
            connections.forEach(session -> session.maintain(now));
        }, 100, 100, TimeUnit.MILLISECONDS);
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                if (connections.size() >= limits.maxConnections()) {
                    socket.close();
                    dropped.increment();
                    LOG.log(System.Logger.Level.WARNING, "Connection limit reached; socket rejected");
                    continue;
                }
                socket.setTcpNoDelay(true);
                var session = new ClientSession(this, socket);
                connections.add(session);
                if (!running) { session.closeSession("Broker stopping", true); continue; }
                try {
                    workers.submit(session::writeLoop);
                    workers.submit(new ConnectionHandler(session));
                } catch (RejectedExecutionException e) {
                    session.closeSession("Broker stopping", true);
                    return;
                }
            } catch (IOException e) {
                if (running) System.getLogger(getClass().getName()).log(System.Logger.Level.ERROR, "Accept failed", e);
            }
        }
    }

    synchronized boolean register(ClientSession session) {
        ClientSession old;
        synchronized (session) {
            if (!running || session.isClosed()) return false;
            old = sessions.get(session.clientId);
            if (old != null && old != session && !Objects.equals(old.username, session.username)) {
                session.reject(2);
                return false;
            }
            sessions.put(session.clientId, session);
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
        if (packet.retain() && !retained.update(packet))
            LOG.log(System.Logger.Level.WARNING, "Retained message dropped: quota reached");
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

    @Override public void close() {
        boolean initiate;
        synchronized (this) {
            initiate = !closed;
            if (initiate) {
                closed = true;
                running = false;
            }
        }
        if (!initiate) { awaitCloseComplete(); return; }
        try {
            if (serverSocket != null) try { serverSocket.close(); } catch (IOException ignored) {}
            join(acceptor, "acceptor");
            maintenance.shutdownNow();
            awaitTermination(maintenance, "maintenance");
            connections.forEach(session -> session.closeSession("Broker shutdown", true));
            workers.shutdownNow();
            awaitTermination(workers, "workers");
        } finally {
            closeComplete.countDown();
        }
    }

    private void awaitCloseComplete() {
        try { closeComplete.await(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static void join(Thread thread, String name) {
        if (thread == null || thread == Thread.currentThread()) return;
        try {
            thread.join(5_000);
            if (thread.isAlive()) LOG.log(System.Logger.Level.WARNING, "Broker {0} thread did not stop", name);
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static void awaitTermination(ExecutorService executor, String name) {
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS))
                LOG.log(System.Logger.Level.WARNING, "Broker {0} executor did not stop", name);
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 1883;
        Path users = Path.of(args.length > 1 ? args[1] : "users.json");
        Path acl = Path.of(args.length > 2 ? args[2] : "acl.json");
        String host = args.length > 3 ? args[3] : "127.0.0.1";
        var tls = Tls.serverFromEnvironment();
        var broker = new BrokerServer(host, port, new AuthService(users, acl), tls, BrokerLimits.fromEnvironment());
        Runtime.getRuntime().addShutdownHook(new Thread(broker::close));
        broker.start();
        System.out.println("FrostMQ listening on " + host + ":" + broker.port() + (tls == null ? "" : " (TLS)"));
        new CountDownLatch(1).await();
    }
}
