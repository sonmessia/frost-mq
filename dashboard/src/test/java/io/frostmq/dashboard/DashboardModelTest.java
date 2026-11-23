package io.frostmq.dashboard;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.*;

class DashboardModelTest {
  private final DashboardModel model = new DashboardModel();

  private boolean apply(String topic, String payload) {
    return model.apply(topic, payload.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void deviceProjectionPreservesStatusAndUsesSiteIdentity() {
    assertTrue(apply("coldchain/site/sensor01/status", "online"));
    assertTrue(
        apply("coldchain/site/sensor01/telemetry", "{\"temp\":9.2,\"hum\":78.5,\"door\":1,\"ts\":1764000000123}"));
    assertEquals(1, model.onlineCount());
    assertEquals(1, model.criticalCount());
    assertEquals(9.2, model.devices().getFirst().temperature());
    assertEquals(1, model.devices().getFirst().door());
    assertTrue(apply("coldchain/other/sensor01/status", "offline"));
    assertEquals(2, model.devices().size());
    assertTrue(apply("coldchain/site/sensor01/status", "offline"));
    assertEquals(0, model.onlineCount());
    assertEquals(9.2, model.devices().getFirst().temperature());
  }

  @Test
  void alertsAreNewestFirstAndBounded() {
    for (int i = 0; i < 1002; i++)
      assertTrue(apply("coldchain/alerts/WARNING",
          "{\"rule\":\"device-offline\",\"device\":\"sensor01\",\"ts\":" + i + "}"));
    assertEquals(1000, model.alerts().size());
    assertEquals(1001, model.alerts().getFirst().timestamp());
    assertEquals("WARNING", model.alerts().getFirst().severity());
  }

  @Test
  void invalidAndCommandMessagesDoNotCorruptState() {
    assertFalse(apply("coldchain/site/sensor01/cmd", "{\"power\":\"on\"}"));
    assertFalse(apply("coldchain/site/sensor01/telemetry", "{}"));
    assertFalse(apply("coldchain/site/sensor01/status", "broken"));
    assertFalse(apply("coldchain/alerts/CRITICAL", "{\"invalid\":true}"));
    assertTrue(model.devices().isEmpty());
    assertTrue(model.alerts().isEmpty());
  }
}
