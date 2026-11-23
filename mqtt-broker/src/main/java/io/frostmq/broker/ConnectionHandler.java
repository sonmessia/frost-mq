package io.frostmq.broker;

import io.frostmq.common.*;
import java.io.IOException;
import java.util.*;

final class ConnectionHandler implements Runnable {
    private static final System.Logger LOG = System.getLogger(ConnectionHandler.class.getName());
    private final ClientSession session;
    private final BrokerServer broker;
    ConnectionHandler(ClientSession session) { this.session = session; broker = session.broker; }

    @Override public void run() {
        session.reader = Thread.currentThread();
        boolean rejected = false;
        try {
            session.socket.setSoTimeout(10_000);
            MqttPacket first = PacketCodec.decode(session.socket.getInputStream());
            if (!(first instanceof ConnectPacket connect)) return;
            if (!accept(connect)) { rejected = true; return; }
            session.socket.setSoTimeout(0);
            while (!session.isClosed()) {
                MqttPacket packet = PacketCodec.decode(session.socket.getInputStream());
                if (packet == null) return;
                session.lastPacketNanos = System.nanoTime();
                switch (packet) {
                    case PublishPacket p -> publish(p);
                    case SubscribePacket p -> subscribe(p);
                    case UnsubscribePacket p -> {
                        p.filters().forEach(filter -> broker.subscriptions().unsubscribe(session, filter));
                        session.enqueue(new UnsubackPacket(p.packetId()));
                    }
                    case PubackPacket p -> session.acknowledge(p.packetId());
                    case PingReqPacket ignored -> session.enqueue(new PingRespPacket());
                    case DisconnectPacket ignored -> { session.closeSession("DISCONNECT", true); return; }
                    default -> { session.closeSession("Unexpected packet"); return; }
                }
            }
        } catch (IOException | RuntimeException e) {
            session.closeSession("Invalid packet or disconnected socket");
        } finally {
            if (!rejected) session.closeSession("Reader stopped");
        }
    }

    private boolean accept(ConnectPacket connect) {
        if (!connect.protocolName().equals("MQTT") || connect.protocolLevel() != 4) {
            session.reject(1); return false;
        }
        if (!connect.cleanSession()) { session.reject(2); return false; }
        var authenticated = broker.auth().authenticate(connect.username(), connect.password());
        if (authenticated.isEmpty()) { session.reject(4); return false; }
        var principal = authenticated.get();
        String id = connect.clientId().isEmpty() ? UUID.randomUUID().toString() : connect.clientId();
        if (principal.role().equals("device") && !id.equals(principal.username())) { session.reject(2); return false; }
        PublishPacket will = connect.willMessage();
        if (will != null && (will.qos() > 1 || !broker.auth().canPublish(principal, will.topic()))) {
            session.reject(4); return false;
        }
        session.clientId = id;
        session.username = principal.username();
        session.principal = principal;
        session.cleanSession = true;
        session.keepAliveSeconds = connect.keepAliveSeconds();
        session.willMessage = will;
        return broker.register(session);
    }

    private void publish(PublishPacket packet) {
        if (packet.qos() > 1) { session.closeSession("QoS 2 not supported"); return; }
        if (!broker.auth().canPublish(session.principal, packet.topic())) {
            LOG.log(System.Logger.Level.WARNING, "Unauthorized PUBLISH dropped");
            if (++session.consecutiveViolations > 5) session.closeSession("Repeated ACL violations");
        } else {
            session.consecutiveViolations = 0;
            broker.route(packet);
        }
        if (!session.isClosed() && packet.qos() == 1) session.enqueue(new PubackPacket(packet.packetId()));
    }

    private void subscribe(SubscribePacket packet) {
        var codes = new ArrayList<Integer>();
        var retained = new LinkedHashMap<String, PublishPacket>();
        var retainedQos = new HashMap<String, Integer>();
        for (var request : packet.subscriptions()) {
            if (!broker.auth().canSubscribe(session.principal, request.filter())) { codes.add(128); continue; }
            int granted = Math.min(request.qos(), 1);
            codes.add(granted);
            broker.subscriptions().subscribe(session, request.filter(), granted);
            for (PublishPacket message : broker.retained().matching(request.filter())) {
                retained.put(message.topic(), message);
                retainedQos.merge(message.topic(), granted, Math::max);
            }
        }
        session.enqueue(new SubackPacket(packet.packetId(), codes));
        retained.forEach((topic, message) -> session.deliver(message, retainedQos.get(topic), true));
    }
}
