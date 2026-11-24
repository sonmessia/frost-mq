package io.frostmq.broker;

import io.frostmq.common.*;
import org.junit.jupiter.api.*;
import java.net.*;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class BrokerProtocolTest {
    private BrokerServer broker;
    private final List<Socket> sockets = new ArrayList<>();
    private static final String STATUS = "coldchain/site/sensor01/status";
    private static final String TELEMETRY = "coldchain/site/sensor01/telemetry";
    @BeforeEach void start() throws Exception {
        broker = new BrokerServer(0, new AuthService(Path.of("../users.json"), Path.of("../acl.json")));
        broker.start();
    }
    @AfterEach void stop() throws Exception {
        for (Socket socket : sockets) socket.close();
        broker.close();
    }
    private Socket raw() throws Exception {
        Socket socket = new Socket("127.0.0.1", broker.port());
        socket.setSoTimeout(3000);
        sockets.add(socket);
        return socket;
    }
    private ConnectPacket options(String id, String user, int keepAlive, PublishPacket will) {
        return new ConnectPacket("MQTT", 4, id, true, keepAlive, user,
                "frostmq-demo".getBytes(StandardCharsets.UTF_8), will);
    }
    private Socket connect(String id, String user) throws Exception { return connect(options(id, user, 30, null)); }
    private Socket connect(ConnectPacket packet) throws Exception {
        Socket socket = raw();
        send(socket, packet);
        assertEquals(new ConnackPacket(false, 0), read(socket));
        return socket;
    }
    private void send(Socket socket, MqttPacket packet) throws Exception { PacketCodec.encode(packet, socket.getOutputStream()); }
    private MqttPacket read(Socket socket) throws Exception { return PacketCodec.decode(socket.getInputStream()); }
    private Socket viewer(String filter, int qos) throws Exception {
        Socket viewer = connect(UUID.randomUUID().toString(), "viewer");
        send(viewer, new SubscribePacket(1, List.of(new SubscribePacket.Subscription(filter, qos))));
        assertEquals(new SubackPacket(1, List.of(Math.min(qos, 1))), read(viewer));
        return viewer;
    }
    private PublishPacket publish(String topic, String payload, int qos, boolean retain, int id) {
        return new PublishPacket(topic, payload.getBytes(StandardCharsets.UTF_8), qos, retain, false, id);
    }
    private void noMessage(Socket socket) throws Exception {
        socket.setSoTimeout(200);
        assertThrows(SocketTimeoutException.class, () -> read(socket));
        socket.setSoTimeout(3000);
    }
    private void awaitRemoved() throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (broker.sessionCount() != 0 && System.nanoTime() < end) Thread.sleep(10);
        assertEquals(0, broker.sessionCount());
    }

    @Test void firstPacketFlagsAndQos2PublishAreRejected() throws Exception {
        Socket first = raw();
        send(first, new PingReqPacket());
        assertNull(read(first));
        Socket invalid = connect("sensor01", "sensor01");
        invalid.getOutputStream().write(new byte[]{(byte) 0x80, 0});
        assertNull(read(invalid));
        Socket qos2 = connect("sensor01", "sensor01");
        send(qos2, publish(TELEMETRY, "x", 2, false, 1));
        assertNull(read(qos2));
        for (byte[] packet : List.of(new byte[]{0x30, 3, 0, 1, '#'}, new byte[]{0x30, 2, 0, 0}, new byte[]{0x50, 0})) {
            Socket socket = connect("sensor01", "sensor01");
            socket.getOutputStream().write(packet);
            assertNull(read(socket));
        }
    }

    @Test void connackRejectionsAndEmptyId() throws Exception {
        var valid = options("sensor01", "sensor01", 30, null);
        for (var entry : List.of(
                Map.entry(new ConnectPacket("MQTT", 5, "a", true, 30, "viewer", valid.password(), null), 1),
                Map.entry(new ConnectPacket("NOPE", 4, "a", true, 30, "viewer", valid.password(), null), 1),
                Map.entry(new ConnectPacket("MQTT", 4, "a", false, 30, "viewer", valid.password(), null), 2),
                Map.entry(options("wrong-id", "sensor01", 30, null), 2),
                Map.entry(new ConnectPacket("MQTT", 4, "a", true, 30, "viewer", new byte[]{1}, null), 4),
                Map.entry(options("sensor01", "sensor01", 30, publish("coldchain/site/sensor02/status", "offline", 0, true, 0)), 4))) {
            Socket socket = raw();
            send(socket, entry.getKey());
            assertEquals(new ConnackPacket(false, entry.getValue()), read(socket));
            assertNull(read(socket));
        }
        Socket empty = connect(options("", "viewer", 0, null));
        assertEquals(1, broker.sessionCount());
        send(empty, new PingReqPacket());
        assertEquals(new PingRespPacket(), read(empty));
    }

    @Test void subscribeAclQosDowngradeAndUnsubscribe() throws Exception {
        Socket device = connect("sensor01", "sensor01");
        send(device, new SubscribePacket(42, List.of(
                new SubscribePacket.Subscription("coldchain/+/sensor01/cmd", 2),
                new SubscribePacket.Subscription("coldchain/+/+/cmd", 1))));
        assertEquals(new SubackPacket(42, List.of(1, 128)), read(device));
        assertEquals(1, broker.subscriptions().filterCount());
        send(device, new UnsubscribePacket(43, List.of("coldchain/+/sensor01/cmd")));
        assertEquals(new UnsubackPacket(43), read(device));
        assertEquals(0, broker.subscriptions().filterCount());
    }

    @Test void qos1RoutingRetainReplayDeletionAndOverlappingFilters() throws Exception {
        Socket device = connect("sensor01", "sensor01");
        Socket viewer = viewer("coldchain/#", 1);
        send(viewer, new SubscribePacket(2, List.of(new SubscribePacket.Subscription("coldchain/+/+/telemetry", 0))));
        assertEquals(new SubackPacket(2, List.of(0)), read(viewer));
        send(device, publish(TELEMETRY, "4.2", 1, true, 42));
        assertEquals(new PubackPacket(42), read(device));
        PublishPacket live = (PublishPacket) read(viewer);
        assertEquals(1, live.qos());
        assertFalse(live.retain());
        assertNotEquals(42, live.packetId());
        send(viewer, new PubackPacket(live.packetId()));
        noMessage(viewer);
        Socket replay = viewer(TELEMETRY, 1);
        PublishPacket retained = (PublishPacket) read(replay);
        assertTrue(retained.retain());
        assertArrayEquals("4.2".getBytes(StandardCharsets.UTF_8), retained.payload());
        send(replay, new PubackPacket(retained.packetId()));
        send(device, publish(TELEMETRY, "", 1, true, 43));
        assertEquals(new PubackPacket(43), read(device));
        assertEquals(0, broker.retained().size());
        Socket deleted = viewer(TELEMETRY, 0);
        noMessage(deleted);
    }

    @Test void duplicateIdAndGracefulDisconnectSuppressWill() throws Exception {
        Socket viewer = viewer(STATUS, 0);
        Socket old = connect(options("sensor01", "sensor01", 30, publish(STATUS, "offline", 1, true, 0)));
        Socket replacement = connect(options("sensor01", "sensor01", 30, publish(STATUS, "offline", 1, true, 0)));
        assertNull(read(old));
        noMessage(viewer);
        send(replacement, new DisconnectPacket());
        assertNull(read(replacement));
        noMessage(viewer);
        assertEquals(0, broker.retained().size());
    }

    @Test void deviceClientIdCannotBeClaimedByAnotherAccount() throws Exception {
        Socket device = connect(options("sensor01", "sensor01", 30, null));
        for (String user : List.of("viewer", "engine")) {
            Socket impostor = raw();
            send(impostor, options("sensor01", user, 30, null));
            assertEquals(new ConnackPacket(false, 2), read(impostor));
            assertNull(read(impostor));
        }
        send(device, new PingReqPacket());
        assertEquals(new PingRespPacket(), read(device));
        assertEquals(1, broker.sessionCount());
        send(device, new DisconnectPacket());
        assertNull(read(device));
        awaitRemoved();
        Socket squat = raw();
        send(squat, options("sensor01", "viewer", 30, null));
        assertEquals(new ConnackPacket(false, 2), read(squat));
        assertNull(read(squat));
    }

    @Test void rejectedClientIdTakeoverKeepsDeviceWillArmed() throws Exception {
        Socket watcher = viewer(STATUS, 0);
        Socket device = connect(options("sensor01", "sensor01", 30, publish(STATUS, "offline", 1, true, 0)));
        Socket impostor = raw();
        send(impostor, options("sensor01", "viewer", 30, null));
        assertEquals(new ConnackPacket(false, 2), read(impostor));
        assertNull(read(impostor));
        device.close();
        PublishPacket will = (PublishPacket) read(watcher);
        assertArrayEquals("offline".getBytes(StandardCharsets.UTF_8), will.payload());
    }

    @Test void liveClientIdCannotBeReplacedByAnotherAccount() throws Exception {
        Socket first = connect("shared-session", "viewer");
        Socket second = raw();
        send(second, options("shared-session", "engine", 30, null));
        assertEquals(new ConnackPacket(false, 2), read(second));
        assertNull(read(second));
        send(first, new PingReqPacket());
        assertEquals(new PingRespPacket(), read(first));
        assertEquals(1, broker.sessionCount());
    }

    @Test void abruptDisconnectAndKeepAlivePublishWillOnce() throws Exception {
        Socket viewer = viewer(STATUS, 0);
        Socket device = connect(options("sensor01", "sensor01", 30, publish(STATUS, "offline", 1, true, 0)));
        device.close();
        PublishPacket will = (PublishPacket) read(viewer);
        assertFalse(will.retain());
        assertArrayEquals("offline".getBytes(StandardCharsets.UTF_8), will.payload());
        noMessage(viewer);
        Socket timed = connect(options("sensor01", "sensor01", 1, publish(STATUS, "offline", 0, true, 0)));
        assertNull(read(timed));
        assertInstanceOf(PublishPacket.class, read(viewer));
        noMessage(viewer);
        assertEquals(1, broker.retained().size());
    }

    @Test void unauthorizedPublishCounterResetsAndClosesOnSixth() throws Exception {
        Socket device = connect("sensor01", "sensor01");
        for (int i = 1; i <= 5; i++) {
            send(device, publish("forbidden/topic", "x", 1, false, i));
            assertEquals(new PubackPacket(i), read(device));
        }
        send(device, publish(TELEMETRY, "valid", 1, false, 6));
        assertEquals(new PubackPacket(6), read(device));
        for (int i = 7; i <= 11; i++) {
            send(device, publish("forbidden/topic", "x", 1, false, i));
            assertEquals(new PubackPacket(i), read(device));
        }
        send(device, publish("forbidden/topic", "x", 1, false, 12));
        assertNull(read(device));
    }

    @Test void idleAndTricklingHandshakesHaveAnAbsoluteTenSecondDeadline() throws Exception {
        Socket idle = raw();
        Socket partial = raw();
        partial.getOutputStream().write(new byte[]{0x10, 100, 0});
        idle.setSoTimeout(12_000);
        partial.setSoTimeout(12_000);
        long start = System.nanoTime();
        assertNull(read(idle));
        assertNull(read(partial));
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(elapsed >= 9500 && elapsed < 11500, "Handshake deadline: " + elapsed + "ms");
    }

    @Test void cleanupAndVirtualThreads() throws Exception {
        Socket viewer = viewer("coldchain/#", 0);
        ClientSession session = broker.subscriptions().match(TELEMETRY).keySet().iterator().next();
        assertTrue(session.reader.isVirtual());
        assertTrue(session.writer.isVirtual());
        send(viewer, new DisconnectPacket());
        assertNull(read(viewer));
        awaitRemoved();
        assertEquals(0, broker.subscriptions().filterCount());
    }

    @Test void boundedQueueDropsQos0AndClosesQos1Idempotently() {
        var session = new ClientSession(broker, new Socket());
        for (int i = 0; i < 1000; i++) assertTrue(session.enqueue(publish(TELEMETRY, "x", 0, false, 0)));
        assertFalse(session.enqueue(publish(TELEMETRY, "x", 0, false, 0)));
        assertEquals(1, session.droppedCount());
        assertEquals(1, broker.droppedCount());
        assertFalse(session.isClosed());
        assertFalse(session.enqueue(publish(TELEMETRY, "x", 1, false, 1)));
        assertTrue(session.isClosed());
        session.closeSession("again");
        assertEquals(0, session.queuedCount());
    }

    @Test void outgoingQos1IsRetransmittedWithSameIdAndDupFlag() throws Exception {
        Socket viewer = viewer(TELEMETRY, 1);
        Socket device = connect("sensor01", "sensor01");
        send(device, publish(TELEMETRY, "x", 1, false, 42));
        assertEquals(new PubackPacket(42), read(device));
        PublishPacket first = (PublishPacket) read(viewer);
        ClientSession session = broker.subscriptions().match(TELEMETRY).keySet().iterator().next();
        session.lastPacketNanos = System.nanoTime();
        session.maintain(System.nanoTime() + TimeUnit.SECONDS.toNanos(6));
        PublishPacket retry = (PublishPacket) read(viewer);
        assertEquals(first.packetId(), retry.packetId());
        assertTrue(retry.dup());
        send(viewer, new PubackPacket(retry.packetId()));
        send(viewer, new PingReqPacket());
        assertEquals(new PingRespPacket(), read(viewer));
        assertEquals(0, session.inFlightCount());
    }

    @Test void packetIdentifierReuseCannotSendAnObsoleteQueuedPublish() throws Exception {
        try (var server = new ServerSocket(0); var peer = new Socket("127.0.0.1", server.getLocalPort());
             var socket = server.accept()) {
            peer.setSoTimeout(2000);
            var session = new ClientSession(broker, socket);
            session.deliver(publish(TELEMETRY, "old", 1, false, 1), 1, false);
            session.acknowledge(1);
            // Force the wraparound boundary without sending 65,535 packets.
            var counter = ClientSession.class.getDeclaredField("nextPacketId");
            counter.setAccessible(true);
            counter.setInt(session, 0);
            session.deliver(publish(TELEMETRY, "new", 1, false, 2), 1, false);
            Thread writer = Thread.ofVirtual().start(session::writeLoop);
            try {
                PublishPacket received = (PublishPacket) PacketCodec.decode(peer.getInputStream());
                assertArrayEquals("new".getBytes(StandardCharsets.UTF_8), received.payload());
                assertEquals(1, received.packetId());
            } finally {
                session.closeSession("test complete", true);
                writer.join(1000);
            }
        }
    }
}
