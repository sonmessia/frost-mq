package io.frostmq.client;

import io.frostmq.broker.*;
import io.frostmq.common.Tls;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class TlsIntegrationTest {
    @TempDir Path temporary;

    @Test void tlsRoundTripAndPlaintextIsRejected() throws Exception {
        Path keyStore = temporary.resolve("server.p12");
        Path certificate = temporary.resolve("server.crt");
        Path trustStore = temporary.resolve("trust.p12");
        keytool("-genkeypair", "-alias", "frostmq", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "1", "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                "-storepass", "changeit", "-keypass", "changeit", "-dname", "CN=localhost",
                "-ext", "SAN=IP:127.0.0.1,dns:localhost", "-noprompt");
        keytool("-exportcert", "-alias", "frostmq", "-keystore", keyStore.toString(),
                "-storepass", "changeit", "-file", certificate.toString());
        keytool("-importcert", "-noprompt", "-alias", "frostmq", "-file", certificate.toString(),
                "-keystore", trustStore.toString(), "-storetype", "PKCS12", "-storepass", "changeit");

        SSLContext server = Tls.server(keyStore, "changeit".toCharArray());
        SSLContext client = Tls.client(trustStore, "changeit".toCharArray());
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
                device.publish("coldchain/site/sensor01/telemetry", new byte[]{1, 2}, 1, false);
                assertArrayEquals(new byte[]{1, 2}, messages.poll(2, TimeUnit.SECONDS));
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
