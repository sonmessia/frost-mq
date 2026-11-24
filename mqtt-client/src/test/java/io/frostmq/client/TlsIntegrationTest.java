package io.frostmq.client;

import io.frostmq.broker.*;
import io.frostmq.common.Tls;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import javax.net.ssl.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class TlsIntegrationTest {
  private record Material(Path keyStore, Path trustStore) {
  }

  @TempDir
  Path temporary;

  @Test
  void tlsRoundTripAndPlaintextIsRejected() throws Exception {
    Material material = material("server", "CN=localhost", "SAN=IP:127.0.0.1,dns:localhost");
    SSLContext server = Tls.server(material.keyStore(), "changeit".toCharArray());
    SSLContext client = Tls.client(material.trustStore(), "changeit".toCharArray());
    try (var broker = new BrokerServer(0,
        new AuthService(Path.of("../users.json"), Path.of("../acl.json")), server)) {
      broker.start();
      var viewer = new BlockingMqttClient();
      var device = new BlockingMqttClient();
      try {
        viewer.connect(new ConnectOptions("127.0.0.1", broker.port(), "viewer-tls", "viewer",
            "frostmq-demo".getBytes(StandardCharsets.UTF_8), 30, null, client));
        var messages = new LinkedBlockingQueue<byte[]>();
        assertEquals(1, viewer.subscribe("coldchain/#", 1, (topic, payload, qos, retained) -> messages.add(payload)));
        device.connect(new ConnectOptions("127.0.0.1", broker.port(), "sensor01", "sensor01",
            "frostmq-demo".getBytes(StandardCharsets.UTF_8), 30, null, client));
        device.publish("coldchain/site/sensor01/telemetry", new byte[] { 1, 2 }, 1, false);
        assertArrayEquals(new byte[] { 1, 2 }, messages.poll(2, TimeUnit.SECONDS));
      } finally {
        viewer.disconnect();
        device.disconnect();
      }

      var plaintext = new BlockingMqttClient();
      try {
        assertThrows(MqttException.class, () -> plaintext.connect(new ConnectOptions(
            "127.0.0.1", broker.port(), "plaintext", "viewer", "frostmq-demo")));
      } finally {
        plaintext.disconnect();
      }
    }
  }

  @Test
  void untrustedServerCertificateIsRejected() throws Exception {
    Material server = material("server", "CN=localhost", "SAN=IP:127.0.0.1,dns:localhost");
    Material unrelated = material("unrelated", "CN=unrelated.invalid", "SAN=dns:unrelated.invalid");
    try (var broker = tlsBroker(Tls.server(server.keyStore(), "changeit".toCharArray()))) {
      broker.start();
      assertHandshakeRejected("127.0.0.1", broker.port(),
          Tls.client(unrelated.trustStore(), "changeit".toCharArray()));
    }
  }

  @Test
  void hostnameOutsideCertificateSanIsRejected() throws Exception {
    Material material = material("wrong-host", "CN=wrong.invalid", "SAN=dns:wrong.invalid");
    try (var broker = tlsBroker(Tls.server(material.keyStore(), "changeit".toCharArray()))) {
      broker.start();
      assertHandshakeRejected("127.0.0.1", broker.port(),
          Tls.client(material.trustStore(), "changeit".toCharArray()));
    }
  }

  @Test
  void commonNameWithoutSanIsRejected() throws Exception {
    Material material = material("cn-only", "CN=localhost", null);
    try (var broker = tlsBroker(Tls.server(material.keyStore(), "changeit".toCharArray()))) {
      broker.start();
      MqttException error = assertHandshakeRejected("localhost", broker.port(),
          Tls.client(material.trustStore(), "changeit".toCharArray()));
      assertTrue(causeMessageContains(error, "lacks a DNS SAN"), error.toString());
    }
  }

  private BrokerServer tlsBroker(SSLContext context) throws Exception {
    return new BrokerServer(0,
        new AuthService(Path.of("../users.json"), Path.of("../acl.json")), context);
  }

  private static MqttException assertHandshakeRejected(String host, int port, SSLContext context) {
    var client = new BlockingMqttClient();
    try {
      MqttException error = assertThrows(MqttException.class, () -> client.connect(new ConnectOptions(
          host, port, "rejected-tls", "viewer", "frostmq-demo".getBytes(StandardCharsets.UTF_8),
          30, null, context)));
      assertTrue(hasCause(error, SSLHandshakeException.class), error.toString());
      return error;
    } finally {
      client.disconnect();
    }
  }

  private Material material(String name, String distinguishedName, String san) throws Exception {
    Path keyStore = temporary.resolve(name + "-server.p12");
    Path certificate = temporary.resolve(name + "-server.crt");
    Path trustStore = temporary.resolve(name + "-trust.p12");
    var generate = new ArrayList<>(List.of("-genkeypair", "-alias", name, "-keyalg", "RSA",
        "-keysize", "2048", "-validity", "1", "-storetype", "PKCS12", "-keystore",
        keyStore.toString(), "-storepass", "changeit", "-keypass", "changeit", "-dname",
        distinguishedName));
    if (san != null)
      generate.addAll(List.of("-ext", san));
    generate.add("-noprompt");
    keytool(generate.toArray(String[]::new));
    keytool("-exportcert", "-alias", name, "-keystore", keyStore.toString(),
        "-storepass", "changeit", "-file", certificate.toString());
    keytool("-importcert", "-noprompt", "-alias", name, "-file", certificate.toString(),
        "-keystore", trustStore.toString(), "-storetype", "PKCS12", "-storepass", "changeit");
    return new Material(keyStore, trustStore);
  }

  private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
    for (Throwable current = error; current != null; current = current.getCause())
      if (type.isInstance(current))
        return true;
    return false;
  }

  private static boolean causeMessageContains(Throwable error, String text) {
    for (Throwable current = error; current != null; current = current.getCause())
      if (current.getMessage() != null && current.getMessage().contains(text))
        return true;
    return false;
  }

  private static void keytool(String... arguments) throws Exception {
    Path tool = Path.of(System.getProperty("java.home"), "bin",
        System.getProperty("os.name").startsWith("Windows") ? "keytool.exe" : "keytool");
    Assumptions.assumeTrue(Files.isExecutable(tool), "keytool is not available");
    var command = new ArrayList<String>();
    command.add(tool.toString());
    command.addAll(List.of(arguments));
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertEquals(0, process.waitFor(), output);
  }
}
