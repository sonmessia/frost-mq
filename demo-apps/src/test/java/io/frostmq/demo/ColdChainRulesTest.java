package io.frostmq.demo;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.*;

class ColdChainRulesTest {
  private final ColdChainRules rules = new ColdChainRules();

  private java.util.List<ColdChainRules.Emission> temp(double value) {
    return rules.onMessage("coldchain/site/sensor01/telemetry",
        ("{\"temp\":" + value + "}").getBytes(StandardCharsets.UTF_8), false);
  }

  @Test
  void strictThresholdsAndHysteresis() {
    assertTrue(temp(8.0).isEmpty());
    var firing = temp(8.01);
    assertEquals("coldchain/alerts/CRITICAL", firing.getFirst().topic());
    assertEquals("{\"power\":\"on\"}", firing.get(1).payload());
    assertTrue(temp(9.0).isEmpty());
    assertTrue(temp(7.0).isEmpty());
    assertTrue(temp(6.0).isEmpty());
    var resolved = temp(5.99);
    assertEquals("coldchain/alerts/RESOLVED", resolved.getFirst().topic());
    assertEquals("{\"power\":\"off\"}", resolved.get(1).payload());
    assertTrue(temp(4.0).isEmpty());
    assertEquals(2, temp(9.0).size());
  }

  @Test
  void retainedStartupAndMalformedPayloadsAreIgnored() {
    assertTrue(
        rules.onMessage("coldchain/site/sensor01/telemetry", "{\"temp\":9}".getBytes(StandardCharsets.UTF_8), true)
            .isEmpty());
    assertEquals(2, temp(9).size());
    assertTrue(
        rules.onMessage("coldchain/site/sensor01/status", "offline".getBytes(StandardCharsets.UTF_8), true).isEmpty());
    assertTrue(
        rules.onMessage("coldchain/site/sensor01/telemetry", "{}".getBytes(StandardCharsets.UTF_8), false).isEmpty());
    assertTrue(temp(Double.NaN).isEmpty());
  }

  @Test
  void sitesAreIndependentAndOfflineAlwaysWarns() {
    assertEquals(2, temp(9).size());
    assertEquals(2,
        rules.onMessage("coldchain/other/sensor01/telemetry", "{\"temp\":9}".getBytes(StandardCharsets.UTF_8), false)
            .size());
    var warning = rules.onMessage("coldchain/site/sensor01/status", "offline".getBytes(StandardCharsets.UTF_8), false);
    assertEquals("coldchain/alerts/WARNING", warning.getFirst().topic());
    assertEquals("device-offline", warning.getFirst().alert().rule());
  }
}
