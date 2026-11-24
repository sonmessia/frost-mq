package io.frostmq.demo;

import io.frostmq.client.*;
import io.frostmq.common.Tls;
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
    private final MqttClient client;
    private final ReconnectingMqttSession session;
    private final Path alertsPath;
    private final BlockingQueue<Event> events = new ArrayBlockingQueue<>(10_000);
    private final ColdChainRules rules = new ColdChainRules();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Thread worker;
    private boolean started;

    public RuleEngine(String host, int port, String password, Path alertsPath) throws IOException {
        this(new BlockingMqttClient(), new ConnectOptions(host, port, "engine-" + UUID.randomUUID(), "engine",
                password.getBytes(StandardCharsets.UTF_8), 30, null, Tls.clientFromEnvironment()), alertsPath);
    }

    RuleEngine(MqttClient client, ConnectOptions options, Path alertsPath) {
        this.client = client;
        this.alertsPath = alertsPath;
        session = new ReconnectingMqttSession(client, options, this::restoreSession, state -> {
            switch (state.status()) {
                case CONNECTED -> LOG.log(System.Logger.Level.INFO, "Rule engine connected and subscribed");
                case RETRYING -> {
                    if (running.get()) LOG.log(System.Logger.Level.WARNING,
                            "Rule engine disconnected; reconnecting", state.cause());
                }
                case CONNECTING -> { }
            }
        });
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
            session.start().toCompletableFuture().get(12, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.log(System.Logger.Level.WARNING,
                    "Rule engine started without a broker connection; reconnecting in background");
        } catch (Exception e) { close(); throw e; }
    }

    private void restoreSession(MqttClient mqtt) throws MqttException {
        MessageListener listener = (topic, payload, qos, retained) -> {
            if (!retained && !events.offer(new Event(topic, payload, false)))
                LOG.log(System.Logger.Level.ERROR, "Rule engine event queue full; message dropped");
        };
        if (mqtt.subscribe("coldchain/+/+/telemetry", 1, listener) == 128 ||
                mqtt.subscribe("coldchain/+/+/status", 1, listener) == 128)
            throw new MqttException("Rule engine subscriptions denied");
    }

    private void process(BufferedWriter csv) {
        try (csv) {
            while (running.get()) {
                Event event = events.take();
                for (var emission : rules.onMessage(event.topic(), event.payload(), event.retained())) {
                    publishWithRetry(emission.topic(), emission.payload().getBytes(StandardCharsets.UTF_8));
                    if (emission.alert() != null) {
                        var alert = emission.alert();
                        String severity = emission.topic().substring(emission.topic().lastIndexOf('/') + 1);
                        csv.write(alert.ts() + "," + severity + "," + CsvFormat.cell(alert.rule()) + ","
                                + CsvFormat.cell(alert.site()) + "," + CsvFormat.cell(alert.device()) + ","
                                + (alert.value() == null ? "" : CsvFormat.cell(Double.toString(alert.value()))) + "\n");
                        csv.flush();
                    }
                }
            }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        catch (IOException e) {
            if (running.get()) LOG.log(System.Logger.Level.ERROR, "Rule engine stopped", e);
            close();
        }
    }

    /** Retries transient publish failures while the connection supervisor restores the session. */
    private void publishWithRetry(String topic, byte[] payload) throws InterruptedException {
        while (running.get()) {
            try {
                client.publish(topic, payload, 1, false);
                return;
            } catch (MqttException e) {
                if (!running.get()) return;
                LOG.log(System.Logger.Level.WARNING, "Rule engine publish failed; will retry", e);
                if (!session.awaitConnected()) return;
            }
        }
    }

    @Override public synchronized void close() {
        running.set(false);
        session.close();
        Thread currentWorker = worker;
        if (currentWorker != null) currentWorker.interrupt();
        join(currentWorker);
    }

    private void join(Thread thread) {
        if (thread == null || Thread.currentThread() == thread) return;
        try { thread.join(2000); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
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
