package io.frostmq.client;

import io.frostmq.broker.*;
import org.junit.jupiter.api.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class BrokerLimitsTest {
  private BrokerServer broker;
  private final List<BlockingMqttClient> clients = new ArrayList<>();

  @AfterEach
  void stop() {
    clients.forEach(BlockingMqttClient::disconnect);
    if (broker != null)
      broker.close();
  }

  @Test
  void connectionLimitRejectsNewSocketsAndKeepsExistingSession() throws Exception {
    start(new BrokerLimits(1, 10, 10, 10));
    var first = client("one", "viewer");
    var second = new BlockingMqttClient();
    clients.add(second);
    assertThrows(MqttException.class, () -> second.connect(
        new ConnectOptions("127.0.0.1", broker.port(), "two", "viewer", "frostmq-demo")));
    assertEquals(1, broker.connectionCount());
    assertEquals(1, broker.rejectedConnectionCount());
    assertEquals(0, broker.outboundDropCount());
    assertEquals(1, first.subscribe("coldchain/#", 1, (topic, payload, qos, retained) -> {
    }));
  }

  @Test
  void subscriptionQuotasApplyPerSessionAndGlobally() throws Exception {
    start(new BrokerLimits(4, 1, 2, 10));
    var one = client("one", "viewer");
    var two = client("two", "viewer");
    var three = client("three", "viewer");
    assertEquals(1, one.subscribe("coldchain/#", 1, (topic, payload, qos, retained) -> {
    }));
    assertEquals(128, one.subscribe("coldchain/+/+/telemetry", 1, (topic, payload, qos, retained) -> {
    }));
    assertEquals(1, two.subscribe("coldchain/#", 1, (topic, payload, qos, retained) -> {
    }));
    assertEquals(128, three.subscribe("coldchain/#", 1, (topic, payload, qos, retained) -> {
    }));
    assertEquals(2, broker.subscriptions().subscriptionCount());
  }

  @Test
  void retainedQuotaDropsNewTopicsButAllowsReplacementAndDeletion() throws Exception {
    start(new BrokerLimits(4, 10, 10, 1));
    var device = client("sensor01", "sensor01");
    device.publish("coldchain/site/sensor01/telemetry", new byte[] { 1 }, 1, true);
    device.publish("coldchain/site/sensor01/status", "offline".getBytes(StandardCharsets.UTF_8), 1, true);
    assertEquals(1, broker.retained().size());
    device.publish("coldchain/site/sensor01/telemetry", new byte[] { 2 }, 1, true);
    assertEquals(1, broker.retained().size());
    device.publish("coldchain/site/sensor01/telemetry", new byte[0], 1, true);
    assertEquals(0, broker.retained().size());
  }

  private void start(BrokerLimits limits) throws Exception {
    broker = new BrokerServer(0, new AuthService(Path.of("../users.json"), Path.of("../acl.json")), limits);
    broker.start();
  }

  private BlockingMqttClient client(String id, String username) throws Exception {
    var client = new BlockingMqttClient();
    clients.add(client);
    client.connect(new ConnectOptions("127.0.0.1", broker.port(), id, username, "frostmq-demo"));
    return client;
  }
}
