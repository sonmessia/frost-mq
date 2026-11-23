package io.frostmq.dashboard;

import com.google.gson.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * UI-independent projection, mutated only on the JavaFX application thread in
 * the app.
 */
public final class DashboardModel {
  public record Device(String site, String id, String status, Double temperature, Double humidity,
      Integer door, long updated) {
  }

  public record Alert(String severity, String rule, String site, String device, Double value, long timestamp) {
  }

  private final Map<String, Device> devices = new LinkedHashMap<>();
  private final ArrayDeque<Alert> alerts = new ArrayDeque<>();

  public List<Device> devices() {
    return List.copyOf(devices.values());
  }

  public List<Alert> alerts() {
    return List.copyOf(alerts);
  }

  public long onlineCount() {
    return devices.values().stream().filter(d -> d.status().equals("online")).count();
  }

  public long criticalCount() {
    return devices.values().stream().filter(d -> d.temperature() != null && d.temperature() > 8).count();
  }

  public boolean apply(String topic, byte[] payload) {
    String[] parts = topic.split("/", -1);
    if (parts.length < 3 || !parts[0].equals("coldchain"))
      return false;
    String text = new String(payload, StandardCharsets.UTF_8);
    try {
      if (parts.length == 3 && parts[1].equals("alerts")) {
        JsonObject json = JsonParser.parseString(text).getAsJsonObject();
        String severity = parts[2];
        if (!Set.of("CRITICAL", "WARNING", "RESOLVED").contains(severity))
          return false;
        alerts.addFirst(new Alert(severity, json.get("rule").getAsString(),
            json.has("site") ? json.get("site").getAsString() : "",
            json.get("device").getAsString(), number(json, "value"), json.get("ts").getAsLong()));
        while (alerts.size() > 1000)
          alerts.removeLast();
        return true;
      }
      if (parts.length != 4)
        return false;
      String key = parts[1] + "/" + parts[2];
      Device previous = devices.getOrDefault(key, new Device(parts[1], parts[2], "unknown", null, null, null, 0));
      if (parts[3].equals("status")) {
        if (!text.equals("online") && !text.equals("offline"))
          return false;
        devices.put(key, new Device(previous.site(), previous.id(), text, previous.temperature(),
            previous.humidity(), previous.door(), System.currentTimeMillis()));
        return true;
      }
      if (!parts[3].equals("telemetry"))
        return false;
      JsonObject json = JsonParser.parseString(text).getAsJsonObject();
      Double temp = number(json, "temp"), hum = number(json, "hum");
      int door = json.get("door").getAsInt();
      long timestamp = json.get("ts").getAsLong();
      if (temp == null || hum == null || (door != 0 && door != 1) || timestamp < 0)
        return false;
      devices.put(key, new Device(previous.site(), previous.id(), previous.status(), temp, hum, door,
          Math.max(timestamp, previous.updated())));
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private static Double number(JsonObject object, String key) {
    JsonElement element = object.get(key);
    if (element == null || element.isJsonNull())
      return null;
    double number = element.getAsDouble();
    if (!Double.isFinite(number))
      throw new IllegalArgumentException("Nonfinite telemetry");
    return number;
  }
}
