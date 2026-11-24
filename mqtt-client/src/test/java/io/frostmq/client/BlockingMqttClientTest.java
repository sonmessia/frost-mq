package io.frostmq.client;

import io.frostmq.broker.*;
import io.frostmq.common.*;
import org.junit.jupiter.api.*;
import javax.net.ssl.SSLContext;
import java.net.*;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class BlockingMqttClientTest {
    private BrokerServer broker;
    private final List<BlockingMqttClient> clients = new ArrayList<>();
    @BeforeEach void start() throws Exception {
        broker = new BrokerServer(0, new AuthService(Path.of("../users.json"), Path.of("../acl.json")));
        broker.start();
    }
    @AfterEach void stop() {
        clients.forEach(BlockingMqttClient::disconnect);
        broker.close();
    }
    private BlockingMqttClient client(String id, String username) throws Exception {
        var client = new BlockingMqttClient();
        clients.add(client);
        client.connect(new ConnectOptions("127.0.0.1", broker.port(), id, username, "frostmq-demo"));
        return client;
    }
    @Test void subscribePublishUnsubscribeAndRetainedDelivery() throws Exception {
        var device = client("sensor01", "sensor01");
        String topic = "coldchain/site/sensor01/telemetry";
        device.publish(topic, new byte[]{1, 2, 3}, 1, true);
        var viewer = client("view", "viewer");
        var messages = new LinkedBlockingQueue<Boolean>();
        assertEquals(1, viewer.subscribe("coldchain/#", 2, (t, p, q, r) -> messages.add(r)));
        assertEquals(Boolean.TRUE, messages.poll(2, TimeUnit.SECONDS));
        device.publish(topic, new byte[]{4}, 0, false);
        assertEquals(Boolean.FALSE, messages.poll(2, TimeUnit.SECONDS));
        viewer.unsubscribe("coldchain/#");
        device.publish(topic, new byte[]{5}, 1, false);
        assertNull(messages.poll(200, TimeUnit.MILLISECONDS));
        assertEquals(128, device.subscribe("coldchain/#", 1, (t, p, q, r) -> fail("ACL denied")));
    }

    @Test void callbacksAreVirtualAndBlockingRequestsFailFast() throws Exception {
        var viewer = client("view", "viewer");
        var device = client("sensor01", "sensor01");
        var checked = new CompletableFuture<Boolean>();
        viewer.subscribe("coldchain/#", 0, (t, p, q, r) -> {
            try {
                assertThrows(MqttException.class, () -> viewer.subscribe("coldchain/site/#", 0, (a, b, c, d) -> {}));
                checked.complete(Thread.currentThread().isVirtual());
            } catch (Throwable error) { checked.completeExceptionally(error); }
        });
        device.publish("coldchain/site/sensor01/telemetry", new byte[]{1}, 1, false);
        assertTrue(checked.get(2, TimeUnit.SECONDS));
    }

    @Test void concurrentQos1PublishesAndRepeatedSubscriptions() throws Exception {
        var viewer = client("view", "viewer");
        var device = client("sensor01", "sensor01");
        var messages = new LinkedBlockingQueue<Integer>();
        viewer.subscribe("coldchain/#", 1, (t, p, q, r) -> messages.add((int) p[0]));
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<Future<?>>();
            for (int i = 0; i < 100; i++) {
                final byte value = (byte) i;
                futures.add(executor.submit(() -> {
                    device.publish("coldchain/site/sensor01/telemetry", new byte[]{value}, 1, false);
                    return null;
                }));
            }
            for (var future : futures) future.get(5, TimeUnit.SECONDS);
        }
        var received = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) received.add(Objects.requireNonNull(messages.poll(2, TimeUnit.SECONDS)));
        assertEquals(100, received.size());
        for (int i = 0; i < 3; i++) {
            viewer.unsubscribe("coldchain/#");
            viewer.subscribe("coldchain/#", 0, (t, p, q, r) -> messages.add(42));
        }
        device.publish("coldchain/site/sensor01/telemetry", new byte[]{1}, 1, false);
        assertEquals(42, messages.poll(2, TimeUnit.SECONDS));
    }

    @Test void keepAliveAndReconnectUseIndependentConnectionState() throws Exception {
        var client = new BlockingMqttClient();
        clients.add(client);
        var options = new ConnectOptions("127.0.0.1", broker.port(), "view", "viewer",
                "frostmq-demo".getBytes(StandardCharsets.UTF_8), 1, null);
        client.connect(options);
        Thread.sleep(2200);
        assertEquals(0, client.subscribe("coldchain/#", 0, (t, p, q, r) -> {}));
        client.disconnect();
        client.connect(options);
        assertEquals(1, client.subscribe("coldchain/#", 1, (t, p, q, r) -> {}));
    }

    @Test void failedAuthenticationAndDisconnectNotification() throws Exception {
        var client = new BlockingMqttClient();
        clients.add(client);
        assertThrows(MqttException.class, () -> client.connect(new ConnectOptions("127.0.0.1", broker.port(), "view", "viewer", "wrong")));
        var disconnected = new CompletableFuture<Throwable>();
        client.setConnectionListener(new ConnectionListener() {
            @Override public void onDisconnected(Throwable cause) { disconnected.complete(cause); }
        });
        client.connect(new ConnectOptions("127.0.0.1", broker.port(), "view", "viewer", "frostmq-demo"));
        broker.close();
        assertNotNull(disconnected.get(2, TimeUnit.SECONDS));
        assertThrows(MqttException.class, () -> client.publish("a", new byte[0], 0, false));
    }

    @Test void qos1TimeoutIsFiveSeconds() throws Exception {
        try (var fakeBroker = new ServerSocket(0); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> server = executor.submit(() -> {
                try (var socket = fakeBroker.accept()) {
                    assertInstanceOf(ConnectPacket.class, PacketCodec.decode(socket.getInputStream()));
                    PacketCodec.encode(new ConnackPacket(false, 0), socket.getOutputStream());
                    assertInstanceOf(PublishPacket.class, PacketCodec.decode(socket.getInputStream()));
                    assertNull(PacketCodec.decode(socket.getInputStream()));
                }
                return null;
            });
            var client = new BlockingMqttClient();
            clients.add(client);
            client.connect(new ConnectOptions("127.0.0.1", fakeBroker.getLocalPort(), "a", null, (String) null));
            long start = System.nanoTime();
            assertThrows(MqttException.class, () -> client.publish("a", new byte[]{1}, 1, false));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsed >= 4800 && elapsed < 6000, "PUBACK timeout: " + elapsed);
            server.get(2, TimeUnit.SECONDS);
        }
    }

    @Test void connackTimeoutIsTenSeconds() throws Exception {
        try (var fakeBroker = new ServerSocket(0); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> server = executor.submit(() -> {
                try (var socket = fakeBroker.accept()) {
                    PacketCodec.decode(socket.getInputStream());
                    assertNull(PacketCodec.decode(socket.getInputStream()));
                }
                return null;
            });
            var client = new BlockingMqttClient();
            clients.add(client);
            long start = System.nanoTime();
            assertThrows(MqttException.class, () -> client.connect(
                    new ConnectOptions("127.0.0.1", fakeBroker.getLocalPort(), "a", null, (String) null)));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsed >= 9500 && elapsed < 11500, "CONNACK timeout: " + elapsed);
            server.get(2, TimeUnit.SECONDS);
        }
    }

    @Test void tlsHandshakeSharesTheOverallTenSecondConnectDeadline() throws Exception {
        try (var fakeBroker = new ServerSocket(0); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> server = executor.submit(() -> {
                try (var socket = fakeBroker.accept()) { Thread.sleep(20_000); }
                return null;
            });
            var client = new BlockingMqttClient();
            clients.add(client);
            long start = System.nanoTime();
            assertThrows(MqttException.class, () -> client.connect(new ConnectOptions(
                    "127.0.0.1", fakeBroker.getLocalPort(), "tls-timeout", null, null,
                    30, null, SSLContext.getDefault())));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsed >= 9_500 && elapsed < 11_000, "TLS CONNECT timeout: " + elapsed);
            server.cancel(true);
        }
    }
}
