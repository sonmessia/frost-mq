package io.frostmq.demo;

import io.frostmq.client.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RuleEngine implements AutoCloseable {
    private record Event(String topic, byte[] payload, boolean retained) {}
    private static final System.Logger LOG = System.getLogger(RuleEngine.class.getName());
    private final BlockingMqttClient client = new BlockingMqttClient();
    private final ConnectOptions options;
    private final Path alertsPath;
    private final BlockingQueue<Event> events = new ArrayBlockingQueue<>(10_000);
    private final ColdChainRules rules = new ColdChainRules();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Thread worker;
    private boolean started;

    public RuleEngine(String host, int port, String password, Path alertsPath) {
        options = new ConnectOptions(host, port, "engine-" + UUID.randomUUID(), "engine", password);
        this.alertsPath = alertsPath;
    }

    public synchronized void start() throws Exception {
        if (started) throw new IllegalStateException("Rule engine cannot be restarted");
        started = true;
        boolean needsHeader = !Files.exists(alertsPath) || Files.size(alertsPath) == 0;
        BufferedWriter csv = Files.newBufferedWriter(alertsPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        if (needsHeader) { csv.write("ts,severity,rule,site,device,value\n"); csv.flush(); }
        running.set(true);
        worker = Thread.ofVirtual().name("frostmq-rule-worker").start(() -> process(csv));
        try {
            client.connect(options);
            MessageListener listener = (topic, payload, qos, retained) -> {
                if (!retained && !events.offer(new Event(topic, payload, false)))
                    LOG.log(System.Logger.Level.ERROR, "Rule engine event queue full; message dropped");
            };
            if (client.subscribe("coldchain/+/+/telemetry", 1, listener) == 128 ||
                    client.subscribe("coldchain/+/+/status", 1, listener) == 128)
                throw new MqttException("Rule engine subscriptions denied");
        } catch (Exception e) { close(); throw e; }
    }

    private void process(BufferedWriter csv) {
        try (csv) {
            while (running.get()) {
                Event event = events.take();
                for (var emission : rules.onMessage(event.topic(), event.payload(), event.retained())) {
                    client.publish(emission.topic(), emission.payload().getBytes(StandardCharsets.UTF_8), 1, false);
                    if (emission.alert() != null) {
                        var alert = emission.alert();
                        String severity = emission.topic().substring(emission.topic().lastIndexOf('/') + 1);
                        csv.write(alert.ts() + "," + severity + "," + cell(alert.rule()) + "," + cell(alert.site())
                                + "," + cell(alert.device()) + "," + (alert.value() == null ? "" : alert.value()) + "\n");
                        csv.flush();
                    }
                }
            }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        catch (IOException | MqttException e) {
            if (running.get()) LOG.log(System.Logger.Level.ERROR, "Rule engine stopped", e);
            running.set(false);
            client.disconnect();
        }
    }

    private static String cell(String value) { return "\"" + value.replace("\"", "\"\"") + "\""; }

    @Override public synchronized void close() {
        running.set(false);
        client.disconnect();
        if (worker != null) {
            worker.interrupt();
            if (Thread.currentThread() != worker) {
                try { worker.join(2000); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        var settings = DemoSettings.parse(args, Set.of("alerts", "duration-seconds"));
        int duration = settings.number("duration-seconds", 0);
        if (duration < 0) throw new IllegalArgumentException("Duration must be nonnegative");
        try (var engine = new RuleEngine(settings.host(), settings.port(), settings.password(),
                Path.of(settings.text("alerts", "alerts.csv")))) {
            Runtime.getRuntime().addShutdownHook(new Thread(engine::close));
            engine.start();
            System.out.println("Rule engine ready. Alerts append to " + settings.text("alerts", "alerts.csv"));
            if (duration == 0) new CountDownLatch(1).await();
            else Thread.sleep(duration * 1000L);
        }
    }
}
