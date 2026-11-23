package io.frostmq.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

class AuthServiceTest {
    @TempDir Path temporary;
    @Test void validatesIndependentSampleHashesAndRoles() throws Exception {
        var auth = new AuthService(Path.of("../users.json"), Path.of("../acl.json"));
        var device = auth.authenticate("sensor01", "frostmq-demo".getBytes(StandardCharsets.UTF_8)).orElseThrow();
        assertEquals("device", device.role());
        assertTrue(auth.canPublish(device, "coldchain/site/sensor01/telemetry"));
        assertFalse(auth.canPublish(device, "coldchain/site/sensor02/telemetry"));
        assertTrue(auth.canSubscribe(device, "coldchain/+/sensor01/cmd"));
        assertFalse(auth.canSubscribe(device, "coldchain/+/+/cmd"));
        assertFalse(auth.canSubscribe(device, "coldchain/#"));
        assertTrue(auth.authenticate("unknown", "frostmq-demo".getBytes(StandardCharsets.UTF_8)).isEmpty());
        assertTrue(auth.authenticate("sensor01", "wrong".getBytes(StandardCharsets.UTF_8)).isEmpty());
        assertTrue(auth.authenticate("sensor01", null).isEmpty());
    }

    @Test void malformedConfigurationFailsClosed() throws Exception {
        for (String json : new String[]{"{\"users\":{}}", "{\"users\":{},\"users\":{}}", "{\"users\": []}",
                "{\"users\": {\"a\":{\"role\":\"missing\"}}}", "{\"users\": {\"a/b\":{}}}"}) {
            Path path = temporary.resolve("users.json");
            Files.writeString(path, json);
            assertThrows(IOException.class, () -> new AuthService(path, Path.of("../acl.json")));
        }
        assertThrows(IOException.class, () -> ConfigJson.parse("{\"a\":\"\\ud800\"}"));
        assertThrows(IOException.class, () -> ConfigJson.parse("[1,]"));
        assertThrows(IOException.class, () -> ConfigJson.parse("{\"a\":01}"));
    }
}
