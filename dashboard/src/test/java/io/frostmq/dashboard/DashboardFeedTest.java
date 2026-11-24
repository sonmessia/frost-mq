package io.frostmq.dashboard;

import io.frostmq.broker.*;
import io.frostmq.client.*;
import org.junit.jupiter.api.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class DashboardFeedTest {
    @Test @Timeout(30) void reconnectsAndReplaysSubscriptionAfterBrokerRestart() throws Exception {
        var auth = new AuthService(Path.of("../users.json"), Path.of("../acl.json"));
        try (var first = new BrokerServer(0, auth)) {
            first.start();
            int port = first.port();
            var states = new LinkedBlockingQueue<DashboardFeed.State>();
            var messages = new LinkedBlockingQueue<String>();
            var options = new ConnectOptions("127.0.0.1", port, "dashboard-feed-test", "viewer", "frostmq-demo");
            try (var feed = new DashboardFeed(new BlockingMqttClient(), options,
                    (topic, payload, qos, retained) -> messages.add(topic + "="
                            + new String(payload, StandardCharsets.UTF_8)), states::add)) {
                feed.start();
                await(states, DashboardFeed.Status.CONNECTED, 5);
                publish(port, "before");
                assertEquals("coldchain/site/sensor01/telemetry=before", messages.poll(3, TimeUnit.SECONDS));

                first.close();
                await(states, DashboardFeed.Status.RETRYING, 5);
                try (var second = new BrokerServer(port, auth)) {
                    second.start();
                    await(states, DashboardFeed.Status.CONNECTED, 10);
                    publish(port, "after");
                    assertEquals("coldchain/site/sensor01/telemetry=after", messages.poll(3, TimeUnit.SECONDS));
                }
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

    private static void await(BlockingQueue<DashboardFeed.State> states, DashboardFeed.Status expected,
                              int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            DashboardFeed.State state = states.poll(250, TimeUnit.MILLISECONDS);
            if (state != null && state.status() == expected) return;
        }
        fail("Dashboard feed never reached " + expected);
    }
}
