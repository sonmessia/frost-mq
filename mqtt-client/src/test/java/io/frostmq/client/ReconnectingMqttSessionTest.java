package io.frostmq.client;

import io.frostmq.broker.*;
import org.junit.jupiter.api.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class ReconnectingMqttSessionTest {
    @Test @Timeout(30) void recoversFromInitialFailureAndReplaysStateAfterRestart() throws Exception {
        var auth = new AuthService(Path.of("../users.json"), Path.of("../acl.json"));
        int port;
        try (var seed = new BrokerServer(0, auth)) {
            seed.start();
            port = seed.port();
        }
        var states = new LinkedBlockingQueue<ReconnectingMqttSession.State>();
        var messages = new LinkedBlockingQueue<String>();
        var options = new ConnectOptions("127.0.0.1", port, "reconnect-viewer", "viewer", "frostmq-demo");
        try (var session = new ReconnectingMqttSession(new BlockingMqttClient(), options,
                mqtt -> {
                    if (mqtt.subscribe("coldchain/#", 1, (topic, payload, qos, retained) ->
                            messages.add(new String(payload, StandardCharsets.UTF_8))) == 128)
                        throw new MqttException("Subscription denied");
                }, states::add)) {
            CompletionStage<Void> firstConnection = session.start();
            await(states, ReconnectingMqttSession.Status.RETRYING, 5);

            try (var first = new BrokerServer(port, auth)) {
                first.start();
                firstConnection.toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertTrue(session.awaitConnected());
                publish(port, "before");
                assertEquals("before", messages.poll(3, TimeUnit.SECONDS));
                first.close();
            }
            await(states, ReconnectingMqttSession.Status.RETRYING, 5);

            try (var second = new BrokerServer(port, auth)) {
                second.start();
                await(states, ReconnectingMqttSession.Status.CONNECTED, 10);
                publish(port, "after");
                assertEquals("after", messages.poll(3, TimeUnit.SECONDS));
            }
        }
    }

    private static void publish(int port, String payload) throws Exception {
        try (var device = new BlockingMqttClient()) {
            device.connect(new ConnectOptions("127.0.0.1", port, "sensor01", "sensor01", "frostmq-demo"));
            device.publish("coldchain/site/sensor01/telemetry",
                    payload.getBytes(StandardCharsets.UTF_8), 1, false);
        }
    }

    private static void await(BlockingQueue<ReconnectingMqttSession.State> states,
                              ReconnectingMqttSession.Status expected, int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            ReconnectingMqttSession.State state = states.poll(250, TimeUnit.MILLISECONDS);
            if (state != null && state.status() == expected) return;
        }
        fail("MQTT session never reached " + expected);
    }
}
