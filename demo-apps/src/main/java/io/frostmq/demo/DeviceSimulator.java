package io.frostmq.demo;

import com.google.gson.*;
import io.frostmq.client.*;
import io.frostmq.common.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class DeviceSimulator implements AutoCloseable {
    private static final Gson GSON = new Gson();
    private static final System.Logger LOG = System.getLogger(DeviceSimulator.class.getName());
    private static final class Node {
        final String id;
        final boolean actuator;
        final BlockingMqttClient client = new BlockingMqttClient();
        final AtomicBoolean cooling = new AtomicBoolean();
        final AtomicBoolean fault = new AtomicBoolean();
        final CompletableFuture<Void> ready = new CompletableFuture<>();
        Node(String id, boolean actuator) { this.id = id; this.actuator = actuator; }
    }
    private final DemoSettings settings;
    private final Map<String, Node> nodes = new LinkedHashMap<>();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean running = new AtomicBoolean();
    private boolean started;

    public DeviceSimulator(String host, int port, String site, int count, String password) {
        if (count < 1) throw new IllegalArgumentException("Node count must be positive");
        if (!TopicMatcher.validTopic(site) || site.contains("/")) throw new IllegalArgumentException("Invalid site");
        settings = new DemoSettings(host, port, site, password, Map.of());
        for (int i = 1; i <= count; i++) {
            String id = String.format(Locale.ROOT, "sensor%02d", i);
            nodes.put(id, new Node(id, i <= 2));
        }
    }

    public synchronized void start() throws Exception {
        if (started) throw new IllegalStateException("Simulator cannot be restarted");
        started = true;
        running.set(true);
        nodes.values().forEach(node -> workers.submit(() -> runNode(node)));
        try {
            CompletableFuture.allOf(nodes.values().stream().map(node -> node.ready).toArray(CompletableFuture[]::new))
                    .get(12, TimeUnit.SECONDS);
        } catch (Exception e) { close(); throw e; }
    }

    public void injectFault(String device) {
        Node node = nodes.get(device);
        if (node == null) throw new IllegalArgumentException("Unknown simulated device");
        node.fault.set(true);
    }

    public boolean cooling(String device) { return nodes.get(device).cooling.get(); }

    private void runNode(Node node) {
        String base = "coldchain/" + settings.site() + "/" + node.id;
        var will = new PublishPacket(base + "/status", bytes("offline"), 1, true, false, 0);
        try {
            node.client.connect(new ConnectOptions(settings.host(), settings.port(), node.id, node.id,
                    bytes(settings.password()), 30, will));
            if (node.actuator) {
                int grant = node.client.subscribe("coldchain/+/" + node.id + "/cmd", 1, (topic, payload, qos, retained) -> {
                    try {
                        String power = JsonParser.parseString(new String(payload, StandardCharsets.UTF_8))
                                .getAsJsonObject().get("power").getAsString();
                        if (power.equals("on") || power.equals("off")) node.cooling.set(power.equals("on"));
                    } catch (RuntimeException e) { LOG.log(System.Logger.Level.WARNING, "Invalid actuator command ignored"); }
                });
                if (grant == 128) throw new MqttException("Actuator subscription denied");
            }
            node.client.publish(base + "/status", bytes("online"), 1, true);
            node.ready.complete(null);
            double temp = 4.0;
            var random = ThreadLocalRandom.current();
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                if (node.fault.getAndSet(false)) temp = 9.5;
                else if (node.cooling.get()) temp = Math.max(4.0, temp - 1.2);
                else temp += (4.0 - temp) * 0.08 + random.nextDouble(-0.25, 0.25);
                var telemetry = new JsonObject();
                telemetry.addProperty("temp", Math.round(temp * 100.0) / 100.0);
                telemetry.addProperty("hum", Math.round(random.nextDouble(72, 84) * 100.0) / 100.0);
                telemetry.addProperty("door", random.nextInt(20) == 0 ? 1 : 0);
                telemetry.addProperty("ts", System.currentTimeMillis());
                node.client.publish(base + "/telemetry", bytes(GSON.toJson(telemetry)), 1, true);
                Thread.sleep(2000);
            }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        catch (Exception e) {
            node.ready.completeExceptionally(e);
            if (running.get()) LOG.log(System.Logger.Level.ERROR, "Simulated node disconnected", e);
        } finally {
            if (!Thread.currentThread().isInterrupted()) {
                try { node.client.publish(base + "/status", bytes("offline"), 1, true); }
                catch (MqttException ignored) {}
            }
            node.client.disconnect();
        }
    }

    @Override public synchronized void close() {
        running.set(false);
        workers.shutdown();
        try {
            if (!workers.awaitTermination(4, TimeUnit.SECONDS)) {
                workers.shutdownNow();
                nodes.values().forEach(node -> node.client.disconnect());
            }
        } catch (InterruptedException e) {
            workers.shutdownNow();
            nodes.values().forEach(node -> node.client.disconnect());
            Thread.currentThread().interrupt();
        }
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    public static void main(String[] args) throws Exception {
        var settings = DemoSettings.parse(args, Set.of("nodes", "fault", "fault-after-seconds", "duration-seconds"));
        int duration = settings.number("duration-seconds", 0);
        int faultAfter = settings.number("fault-after-seconds", 6);
        if (duration < 0 || faultAfter < 0) throw new IllegalArgumentException("Durations must be nonnegative");
        try (var simulator = new DeviceSimulator(settings.host(), settings.port(), settings.site(),
                settings.number("nodes", 5), settings.password())) {
            Runtime.getRuntime().addShutdownHook(new Thread(simulator::close));
            simulator.start();
            System.out.println("Sensors online. sensor01 and sensor02 include cooling actuators.");
            String fault = settings.text("fault", "");
            boolean injected = fault.isEmpty();
            long start = System.nanoTime();
            while (duration == 0 || System.nanoTime() - start < TimeUnit.SECONDS.toNanos(duration)) {
                if (!injected && System.nanoTime() - start >= TimeUnit.SECONDS.toNanos(faultAfter)) {
                    simulator.injectFault(fault);
                    injected = true;
                    System.out.println("Temperature fault injected.");
                }
                Thread.sleep(250);
            }
        }
    }
}
