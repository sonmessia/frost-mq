package io.frostmq.demo;

import com.google.gson.*;
import io.frostmq.broker.*;
import io.frostmq.client.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class ColdChainIntegrationTest {
    @TempDir Path temporary;
    @Test @Timeout(20) void faultTriggersCoolingAndResolvesThroughRealMqttAndCsv() throws Exception {
        try (var broker = new BrokerServer(0, new AuthService(Path.of("../users.json"), Path.of("../acl.json")))) {
            broker.start();
            try (var viewer = new BlockingMqttClient();
                 var engine = new RuleEngine("127.0.0.1", broker.port(), "frostmq-demo", temporary.resolve("alerts.csv"));
                 var simulator = new DeviceSimulator("127.0.0.1", broker.port(), "site", 2, "frostmq-demo")) {
                var alerts = new LinkedBlockingQueue<String>();
                viewer.connect(new ConnectOptions("127.0.0.1", broker.port(), "test-viewer", "viewer", "frostmq-demo"));
                viewer.subscribe("coldchain/alerts/+", 1, (t, p, q, r) -> {
                    assertEquals("sensor01", JsonParser.parseString(new String(p, StandardCharsets.UTF_8)).getAsJsonObject().get("device").getAsString());
                    alerts.add(t);
                });
                engine.start();
                simulator.start();
                simulator.injectFault("sensor01");
                assertEquals("coldchain/alerts/CRITICAL", alerts.poll(5, TimeUnit.SECONDS));
                assertEquals("coldchain/alerts/RESOLVED", alerts.poll(10, TimeUnit.SECONDS));
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (simulator.cooling("sensor01") && System.nanoTime() < end) Thread.sleep(10);
                assertFalse(simulator.cooling("sensor01"));
                String csv = Files.readString(temporary.resolve("alerts.csv"));
                assertTrue(csv.contains("CRITICAL,\"temp-high\",\"site\",\"sensor01\""));
                assertTrue(csv.contains("RESOLVED,\"temp-high\",\"site\",\"sensor01\""));
            }
        }
    }

    @Test @Timeout(30) void engineReconnectsAndResumesAlertsAfterBrokerRestart() throws Exception {
        Path csvPath = temporary.resolve("reconnect.csv");
        var auth = new AuthService(Path.of("../users.json"), Path.of("../acl.json"));
        var first = new BrokerServer(0, auth);
        first.start();
        int port = first.port();
        try (var engine = new RuleEngine("127.0.0.1", port, "frostmq-demo", csvPath)) {
            engine.start();
            first.close();
            try (var second = new BrokerServer(port, auth)) {
                second.start();
                try (var device = new BlockingMqttClient()) {
                    device.connect(new ConnectOptions("127.0.0.1", port, "sensor01", "sensor01", "frostmq-demo"));
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    String csv = "";
                    while (System.nanoTime() < deadline) {
                        device.publish("coldchain/site/sensor01/telemetry",
                                "{\"temp\":9.0}".getBytes(StandardCharsets.UTF_8), 1, false);
                        if (Files.exists(csvPath)) {
                            csv = Files.readString(csvPath);
                            if (csv.contains("temp-high")) break;
                        }
                        Thread.sleep(250);
                    }
                    assertTrue(csv.contains("CRITICAL,\"temp-high\",\"site\",\"sensor01\""), csv);
                }
            }
        }
    }

    @Test @Timeout(30) void simulatorReconnectsAndRestoresTelemetryStatusAndCommands() throws Exception {
        var auth = new AuthService(Path.of("../users.json"), Path.of("../acl.json"));
        try (var first = new BrokerServer(0, auth)) {
            first.start();
            int port = first.port();
            try (var simulator = new DeviceSimulator("127.0.0.1", port, "site", 1, "frostmq-demo")) {
                simulator.start();
                first.close();
                try (var second = new BrokerServer(port, auth);
                     var viewer = new BlockingMqttClient();
                     var engine = new BlockingMqttClient()) {
                    second.start();
                    viewer.connect(new ConnectOptions("127.0.0.1", port, "restart-viewer", "viewer", "frostmq-demo"));
                    var restored = new LinkedBlockingQueue<String>();
                    assertEquals(1, viewer.subscribe("coldchain/site/sensor01/#", 1,
                            (topic, payload, qos, retained) -> restored.add(topic + "="
                                    + new String(payload, StandardCharsets.UTF_8))));
                    engine.connect(new ConnectOptions("127.0.0.1", port, "restart-engine", "engine", "frostmq-demo"));

                    boolean online = false, telemetry = false;
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while ((!online || !telemetry) && System.nanoTime() < deadline) {
                        String message = restored.poll(500, TimeUnit.MILLISECONDS);
                        if (message == null) continue;
                        online |= message.equals("coldchain/site/sensor01/status=online");
                        telemetry |= message.startsWith("coldchain/site/sensor01/telemetry=");
                    }
                    assertTrue(online, "online status was not restored after reconnect");
                    assertTrue(telemetry, "telemetry did not resume after reconnect");

                    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (!simulator.cooling("sensor01") && System.nanoTime() < deadline) {
                        engine.publish("coldchain/site/sensor01/cmd",
                                "{\"power\":\"on\"}".getBytes(StandardCharsets.UTF_8), 1, false);
                        Thread.sleep(100);
                    }
                    assertTrue(simulator.cooling("sensor01"), "actuator subscription was not restored");
                }
            }
        }
    }

    @Test @Timeout(20) void untrustedTopicLevelCannotInjectSpreadsheetFormula() throws Exception {
        try (var broker = new BrokerServer(0, new AuthService(Path.of("../users.json"), Path.of("../acl.json")))) {
            broker.start();
            Path csvPath = temporary.resolve("injection.csv");
            try (var engine = new RuleEngine("127.0.0.1", broker.port(), "frostmq-demo", csvPath);
                 var device = new BlockingMqttClient()) {
                engine.start();
                device.connect(new ConnectOptions("127.0.0.1", broker.port(), "sensor01", "sensor01", "frostmq-demo"));
                device.publish("coldchain/=1/sensor01/telemetry",
                        "{\"temp\":9.0}".getBytes(StandardCharsets.UTF_8), 1, false);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                String csv = "";
                while (System.nanoTime() < deadline) {
                    if (Files.exists(csvPath)) {
                        csv = Files.readString(csvPath);
                        if (csv.contains("temp-high")) break;
                    }
                    Thread.sleep(25);
                }
                assertTrue(csv.contains("\"'=1\""), csv);
                assertFalse(csv.contains("\"=1\""), csv);
            }
        }
    }

}
