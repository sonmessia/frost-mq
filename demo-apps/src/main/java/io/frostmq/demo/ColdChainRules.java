package io.frostmq.demo;

import com.google.gson.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Single-worker state machine; keyed by both site and device. Thresholds are deliberately strict. */
public final class ColdChainRules {
    public record Alert(String rule, String device, Double value, long ts, String site) {}
    public record Emission(String topic, String payload, Alert alert) {}
    private final Map<String, Boolean> firing = new HashMap<>();
    private final Gson gson = new Gson();

    public List<Emission> onMessage(String topic, byte[] payload, boolean retained) {
        if (retained) return List.of();
        String[] parts = topic.split("/", -1);
        if (parts.length != 4 || !parts[0].equals("coldchain")) return List.of();
        String site = parts[1], device = parts[2], kind = parts[3];
        String text = new String(payload, StandardCharsets.UTF_8);
        if (kind.equals("status")) {
            if (!text.equals("offline")) return List.of();
            return List.of(alert("WARNING", "device-offline", site, device, null));
        }
        if (!kind.equals("telemetry")) return List.of();
        double temp;
        try { temp = JsonParser.parseString(text).getAsJsonObject().get("temp").getAsDouble(); }
        catch (RuntimeException e) { return List.of(); }
        if (!Double.isFinite(temp)) return List.of();
        String key = site + "/" + device;
        boolean active = firing.getOrDefault(key, false);
        if (!active && temp > 8.0) {
            firing.put(key, true);
            return List.of(alert("CRITICAL", "temp-high", site, device, temp),
                    new Emission("coldchain/" + key + "/cmd", "{\"power\":\"on\"}", null));
        }
        if (active && temp < 6.0) {
            firing.put(key, false);
            return List.of(alert("RESOLVED", "temp-high", site, device, temp),
                    new Emission("coldchain/" + key + "/cmd", "{\"power\":\"off\"}", null));
        }
        return List.of();
    }

    private Emission alert(String severity, String rule, String site, String device, Double value) {
        var alert = new Alert(rule, device, value, System.currentTimeMillis(), site);
        return new Emission("coldchain/alerts/" + severity, gson.toJson(alert), alert);
    }
}
