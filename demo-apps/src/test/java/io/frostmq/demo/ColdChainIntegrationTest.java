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
}
