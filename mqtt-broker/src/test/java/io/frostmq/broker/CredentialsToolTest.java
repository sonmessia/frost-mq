package io.frostmq.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class CredentialsToolTest {
  @TempDir
  Path temporary;

  @Test
  void provisionsAndRotatesAccountsWithRandomSalts() throws Exception {
    Path users = temporary.resolve("users.json");
    provision(users, "first-test-password");
    var auth = new AuthService(users, Path.of("../acl.json"));
    assertTrue(auth.authenticate("sensor11", "first-test-password".getBytes(StandardCharsets.UTF_8)).isPresent());
    String first = Files.readString(users);
    provision(users, "first-test-password");
    assertNotEquals(first, Files.readString(users));
    provision(users, "replacement-test-password");
    auth = new AuthService(users, Path.of("../acl.json"));
    assertTrue(auth.authenticate("sensor11", "first-test-password".getBytes(StandardCharsets.UTF_8)).isEmpty());
    assertTrue(auth.authenticate("sensor11", "replacement-test-password".getBytes(StandardCharsets.UTF_8)).isPresent());
    if (Files.getFileStore(users).supportsFileAttributeView("posix"))
      assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(users));
  }

  private void provision(Path users, String password) throws Exception {
    var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-cp", System.getProperty("java.class.path"), CredentialsTool.class.getName(), users.toString(), "sensor11",
        "device");
    builder.environment().put("FROSTMQ_PASSWORD", password);
    Process process = builder.redirectErrorStream(true).start();
    try {
      assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue(), new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      process.destroyForcibly();
    }
  }
}
