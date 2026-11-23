package io.frostmq.dashboard;

import javafx.application.*;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.WritableImage;
import javafx.stage.Window;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.nio.file.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Opt-in real JavaFX application test, run by scripts/demo_smoke.py against the
 * live demo.
 */
@EnabledIfEnvironmentVariable(named = "FROSTMQ_GUI_TEST", matches = "1")
class DashboardUiTest {
  private Thread launcher;

  private static <T> T fx(Callable<T> action) throws Exception {
    var result = new CompletableFuture<T>();
    Platform.runLater(() -> {
      try {
        result.complete(action.call());
      } catch (Throwable e) {
        result.completeExceptionally(e);
      }
    });
    return result.get(3, TimeUnit.SECONDS);
  }

  private static void await(Callable<Boolean> condition, int seconds) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
    while (System.nanoTime() < deadline) {
      try {
        if (condition.call())
          return;
      } catch (IllegalStateException ignored) {
      }
      Thread.sleep(50);
    }
    fail("JavaFX condition timed out");
  }

  private static Scene scene() {
    return Window.getWindows().stream().filter(Window::isShowing).findFirst().orElseThrow().getScene();
  }

  @Test
  @Timeout(25)
  void liveFleetAlertsNavigationAndRendering() throws Exception {
    launcher = Thread.ofPlatform().name("dashboard-test-launcher").start(() -> Application.launch(DashboardApp.class,
        "--port=" + System.getenv().getOrDefault("FROSTMQ_GUI_PORT", "1883")));
    await(() -> fx(() -> !Window.getWindows().isEmpty()), 8);
    await(() -> fx(() -> ((TableView<?>) scene().lookup("#device-table")).getItems().size() >= 2
        && ((Label) scene().lookup("#connection-status")).getText().startsWith("Live")), 5);
    fx(() -> {
      ((ToggleButton) scene().lookup("#alerts-nav")).fire();
      return null;
    });
    await(() -> fx(() -> ((ListView<?>) scene().lookup("#alert-list")).getItems().stream()
        .anyMatch(item -> ((DashboardModel.Alert) item).severity().equals("CRITICAL"))), 12);
    fx(() -> {
      ((ToggleButton) scene().lookup("#fleet-nav")).fire();
      return null;
    });
    fx(() -> {
      var table = (TableView<?>) scene().lookup("#device-table");
      assertTrue(table.getItems().stream().anyMatch(item -> {
        var device = (DashboardModel.Device) item;
        return device.id().equals("sensor01") && device.status().equals("online") && device.temperature() > 8;
      }));
      snapshot(scene(), Path.of("../artifacts/dashboard-fleet.png"));
      ((ToggleButton) scene().lookup("#alerts-nav")).fire();
      return null;
    });
    await(() -> fx(() -> ((ListView<?>) scene().lookup("#alert-list")).getItems().stream()
        .anyMatch(item -> ((DashboardModel.Alert) item).severity().equals("RESOLVED"))), 10);
    fx(() -> {
      snapshot(scene(), Path.of("../artifacts/dashboard-alerts.png"));
      return null;
    });
  }

  private static void snapshot(Scene scene, Path path) throws Exception {
    scene.getRoot().applyCss();
    scene.getRoot().layout();
    WritableImage image = scene.snapshot(null);
    int width = (int) image.getWidth(), height = (int) image.getHeight();
    var rendered = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
    var reader = image.getPixelReader();
    for (int y = 0; y < height; y++)
      for (int x = 0; x < width; x++)
        rendered.setRGB(x, y, reader.getArgb(x, y));
    Files.createDirectories(path.toAbsolutePath().getParent());
    assertTrue(ImageIO.write(rendered, "png", path.toFile()));
  }

  @AfterEach
  void close() throws Exception {
    try {
      Platform.exit();
    } catch (IllegalStateException ignored) {
    }
    if (launcher != null)
      launcher.join(5000);
  }
}
