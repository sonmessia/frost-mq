package io.frostmq.demo;

import com.google.gson.*;
import io.frostmq.client.*;
import io.frostmq.common.*;
import javax.net.ssl.SSLContext;
import java.io.IOException;
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
    volatile ReconnectingMqttSession session;
    double temperature = 4.0;

    Node(String id, boolean actuator) {
      this.id = id;
      this.actuator = actuator;
    }
  }

  private final DemoSettings settings;
  private final SSLContext tls;
  private final Map<String, Node> nodes = new LinkedHashMap<>();
  private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
  private final AtomicBoolean running = new AtomicBoolean();
  private boolean started;

  public DeviceSimulator(String host, int port, String site, int count, String password) throws IOException {
    if (count < 1)
      throw new IllegalArgumentException("Node count must be positive");
    if (!TopicMatcher.validTopic(site) || site.contains("/"))
      throw new IllegalArgumentException("Invalid site");
    settings = new DemoSettings(host, port, site, password, Map.of());
    tls = Tls.clientFromEnvironment();
    for (int i = 1; i <= count; i++) {
      String id = String.format(Locale.ROOT, "sensor%02d", i);
      nodes.put(id, new Node(id, i <= 2));
    }
  }

  public synchronized void start() throws Exception {
    if (started)
      throw new IllegalStateException("Simulator cannot be restarted");
    started = true;
    running.set(true);
    nodes.values().forEach(node -> workers.submit(() -> runNode(node)));
    try {
      CompletableFuture.allOf(nodes.values().stream().map(node -> node.ready).toArray(CompletableFuture[]::new))
          .get(12, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      LOG.log(System.Logger.Level.WARNING,
          "Simulator started before every node connected; reconnecting in background");
    } catch (Exception e) {
      close();
      throw e;
    }
  }

  public void injectFault(String device) {
    Node node = nodes.get(device);
    if (node == null)
      throw new IllegalArgumentException("Unknown simulated device");
    node.fault.set(true);
  }

  public boolean cooling(String device) {
    return nodes.get(device).cooling.get();
  }

  private void runNode(Node node) {
    String base = "coldchain/" + settings.site() + "/" + node.id;
    var will = new PublishPacket(base + "/status", bytes("offline"), 1, true, false, 0);
    var options = new ConnectOptions(settings.host(), settings.port(), node.id, node.id,
        bytes(settings.password()), 30, will, tls);
    node.session = new ReconnectingMqttSession(node.client, options,
        mqtt -> restoreNode(node, base, mqtt), state -> {
          switch (state.status()) {
            case CONNECTED -> LOG.log(System.Logger.Level.INFO,
                "Simulated node " + node.id + " connected");
            case RETRYING -> {
              if (running.get())
                LOG.log(System.Logger.Level.WARNING,
                    "Simulated node " + node.id + " disconnected; reconnecting", state.cause());
            }
            case CONNECTING -> {
            }
          }
        });
    node.session.start().whenComplete((ignored, error) -> {
      if (error == null)
        node.ready.complete(null);
      else
        node.ready.completeExceptionally(error);
    });
    try {
      if (!node.session.awaitConnected())
        return;
      while (running.get() && !Thread.currentThread().isInterrupted()) {
        try {
          publishTelemetry(node, base);
          Thread.sleep(2000);
        } catch (MqttException e) {
          if (!node.session.awaitConnected())
            return;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      if (!node.ready.isDone())
        node.ready.completeExceptionally(new CancellationException("Simulator stopped before node connected"));
      node.session.close();
    }
  }

  private void restoreNode(Node node, String base, MqttClient mqtt) throws MqttException {
    if (node.actuator) {
      int grant = mqtt.subscribe("coldchain/+/" + node.id + "/cmd", 1,
          (topic, payload, qos, retained) -> applyCommand(node, payload));
      if (grant == 128)
        throw new MqttException("Actuator subscription denied");
    }
    mqtt.publish(base + "/status", bytes("online"), 1, true);
  }

  private static void applyCommand(Node node, byte[] payload) {
    try {
      String power = JsonParser.parseString(new String(payload, StandardCharsets.UTF_8))
          .getAsJsonObject().get("power").getAsString();
      if (power.equals("on") || power.equals("off"))
        node.cooling.set(power.equals("on"));
    } catch (RuntimeException e) {
      LOG.log(System.Logger.Level.WARNING, "Invalid actuator command ignored");
    }
  }

  private static void publishTelemetry(Node node, String base) throws MqttException {
    var random = ThreadLocalRandom.current();
    if (node.fault.getAndSet(false))
      node.temperature = 9.5;
    else if (node.cooling.get())
      node.temperature = Math.max(4.0, node.temperature - 1.2);
    else
      node.temperature += (4.0 - node.temperature) * 0.08 + random.nextDouble(-0.25, 0.25);
    var telemetry = new JsonObject();
    telemetry.addProperty("temp", Math.round(node.temperature * 100.0) / 100.0);
    telemetry.addProperty("hum", Math.round(random.nextDouble(72, 84) * 100.0) / 100.0);
    telemetry.addProperty("door", random.nextInt(20) == 0 ? 1 : 0);
    telemetry.addProperty("ts", System.currentTimeMillis());
    node.client.publish(base + "/telemetry", bytes(GSON.toJson(telemetry)), 1, true);
  }

  @Override
  public synchronized void close() {
    running.set(false);
    nodes.values().forEach(node -> {
      try {
        node.client.publish("coldchain/" + settings.site() + "/" + node.id + "/status",
            bytes("offline"), 0, true);
      } catch (MqttException ignored) {
      }
    });
    nodes.values().forEach(node -> {
      if (node.session != null)
        node.session.close();
    });
    workers.shutdownNow();
    try {
      workers.awaitTermination(4, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  public static void main(String[] args) throws Exception {
    var settings = DemoSettings.parse(args, Set.of("nodes", "fault", "fault-after-seconds", "duration-seconds"));
    int duration = settings.number("duration-seconds", 0);
    int faultAfter = settings.number("fault-after-seconds", 6);
    if (duration < 0 || faultAfter < 0)
      throw new IllegalArgumentException("Durations must be nonnegative");
    try (var simulator = new DeviceSimulator(settings.host(), settings.port(), settings.site(),
        settings.number("nodes", 5), settings.password())) {
      Runtime.getRuntime().addShutdownHook(new Thread(simulator::close));
      simulator.start();
      System.out.println("Sensor simulator started. sensor01 and sensor02 include cooling actuators.");
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
