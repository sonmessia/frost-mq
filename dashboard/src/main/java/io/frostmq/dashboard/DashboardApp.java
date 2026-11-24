package io.frostmq.dashboard;

import io.frostmq.client.*;
import io.frostmq.common.Tls;
import javafx.application.*;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.*;
import javafx.geometry.*;
import javafx.scene.*;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.Stage;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

public final class DashboardApp extends Application {
  private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss")
      .withZone(ZoneId.systemDefault());
  private final DashboardModel model = new DashboardModel();
  private final BlockingMqttClient client = new BlockingMqttClient();
  private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
  private final ObservableList<DashboardModel.Device> devices = FXCollections.observableArrayList();
  private final ObservableList<DashboardModel.Alert> alerts = FXCollections.observableArrayList();
  private final Label connection = new Label("Disconnected");
  private final Label deviceCount = new Label("0");
  private final Label onlineCount = new Label("0");
  private final Label criticalCount = new Label("0");
  private final Label title = new Label("Cold-chain overview");
  private final Label subtitle = new Label("Live conditions across your refrigerated fleet");
  private final Button reconnect = new Button("Connect");
  private final BorderPane content = new BorderPane();
  private volatile boolean closing;
  private Node deviceScreen;
  private Node alertScreen;

  @Override
  public void start(Stage stage) {
    var root = new BorderPane();
    root.getStyleClass().add("app");
    root.setLeft(sidebar());
    var main = new VBox(24);
    main.setPadding(new Insets(32));
    title.getStyleClass().add("page-title");
    subtitle.getStyleClass().add("muted");
    var heading = new VBox(8, title, subtitle);
    var top = new HBox(24, heading, new Region(), connection, reconnect);
    HBox.setHgrow(top.getChildren().get(1), Priority.ALWAYS);
    top.setAlignment(Pos.CENTER_LEFT);
    connection.getStyleClass().add("connection");
    connection.setId("connection-status");
    reconnect.getStyleClass().add("primary-button");
    reconnect.setOnAction(event -> connect());
    deviceScreen = devicesScreen();
    alertScreen = alertsScreen();
    content.setCenter(deviceScreen);
    VBox.setVgrow(content, Priority.ALWAYS);
    main.getChildren().addAll(top, content);
    root.setCenter(main);
    Scene scene = new Scene(root, 1240, 800);
    scene.getStylesheets().add(Objects.requireNonNull(getClass().getResource("dashboard.css")).toExternalForm());
    stage.setTitle("FrostMQ · Cold-chain monitor");
    stage.setMinWidth(960);
    stage.setMinHeight(600);
    stage.setScene(scene);
    stage.show();
    client.setConnectionListener(new ConnectionListener() {
      @Override
      public void onDisconnected(Throwable cause) {
        Platform.runLater(() -> {
          if (!closing) {
            connection.setText(cause == null ? "Disconnected" : "Connection lost");
            reconnect.setDisable(false);
            reconnect.setText("Reconnect");
          }
        });
      }
    });
    connect();
  }

  private Node sidebar() {
    var brand = new Label("FROSTMQ");
    brand.getStyleClass().add("brand");
    var caption = new Label("COLD-CHAIN OPERATIONS");
    caption.getStyleClass().add("sidebar-caption");
    var group = new ToggleGroup();
    var fleet = new ToggleButton("Device fleet");
    var log = new ToggleButton("Alert stream");
    fleet.setId("fleet-nav");
    log.setId("alerts-nav");
    fleet.setToggleGroup(group);
    log.setToggleGroup(group);
    fleet.setSelected(true);
    for (var button : List.of(fleet, log)) {
      button.getStyleClass().add("nav-button");
      button.setMaxWidth(Double.MAX_VALUE);
    }
    fleet.setOnAction(event -> {
      fleet.setSelected(true);
      content.setCenter(deviceScreen);
      title.setText("Cold-chain overview");
      subtitle.setText("Live conditions across your refrigerated fleet");
    });
    log.setOnAction(event -> {
      log.setSelected(true);
      content.setCenter(alertScreen);
      title.setText("Alert stream");
      subtitle.setText("Temperature excursions, recoveries, and device disconnects");
    });
    var spacer = new Region();
    VBox.setVgrow(spacer, Priority.ALWAYS);
    var footer = new Label("MQTT 3.1.1\nQoS 0 / 1 · Clean sessions\nJava 25 · Virtual threads");
    footer.getStyleClass().add("sidebar-footer");
    var side = new VBox(16, brand, caption, new Separator(), fleet, log, spacer, footer);
    side.setPadding(new Insets(32, 24, 32, 24));
    side.setPrefWidth(224);
    side.getStyleClass().add("sidebar");
    return side;
  }

  private Node devicesScreen() {
    var metrics = new HBox(16,
        metric("REGISTERED DEVICES", deviceCount, "Sensors seen by the monitor"),
        metric("ONLINE NOW", onlineCount, "Connected and reporting"),
        metric("ABOVE 8 °C", criticalCount, "Requires cooling intervention"));
    metrics.getChildren().forEach(node -> HBox.setHgrow(node, Priority.ALWAYS));
    var table = new TableView<DashboardModel.Device>(devices);
    table.setId("device-table");
    table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
    table.setPlaceholder(new Label("No devices yet.\nStart DeviceSimulator to see live cold-chain telemetry."));
    table.getColumns().add(column("SITE", DashboardModel.Device::site));
    table.getColumns().add(column("DEVICE", DashboardModel.Device::id));
    var status = column("STATUS", DashboardModel.Device::status);
    status.setCellFactory(ignored -> new TableCell<>() {
      @Override
      protected void updateItem(String value, boolean empty) {
        super.updateItem(value, empty);
        setText(null);
        if (empty || value == null) {
          setGraphic(null);
          return;
        }
        var label = new Label(value.toUpperCase(Locale.ROOT));
        label.getStyleClass().addAll("status-pill", "status-" + value);
        setGraphic(label);
      }
    });
    table.getColumns().add(status);
    var temperature = column("TEMP °C", DashboardModel.Device::temperature);
    temperature.setCellFactory(ignored -> new TableCell<>() {
      @Override
      protected void updateItem(Double value, boolean empty) {
        super.updateItem(value, empty);
        setText(empty || value == null ? null : String.format(Locale.ROOT, "%.1f °C", value));
        getStyleClass().remove("temperature-critical");
        if (!empty && value != null && value > 8)
          getStyleClass().add("temperature-critical");
      }
    });
    table.getColumns().add(temperature);
    table.getColumns()
        .add(column("HUMIDITY", d -> d.humidity() == null ? "—" : String.format(Locale.ROOT, "%.1f %%", d.humidity())));
    table.getColumns().add(column("DOOR", d -> d.door() == null ? "—" : d.door() == 1 ? "Open" : "Closed"));
    table.getColumns()
        .add(column("UPDATED", d -> d.updated() == 0 ? "—" : TIME.format(Instant.ofEpochMilli(d.updated()))));
    var section = new Label("DEVICE FLEET");
    section.getStyleClass().add("section-label");
    var note = new Label("Healthy target: 4 °C  ·  Alert above 8 °C  ·  Recovery below 6 °C");
    note.getStyleClass().add("muted");
    var screen = new VBox(16, metrics, section, table, note);
    VBox.setVgrow(table, Priority.ALWAYS);
    return screen;
  }

  private Node alertsScreen() {
    var list = new ListView<DashboardModel.Alert>(alerts);
    list.setId("alert-list");
    list.setPlaceholder(new Label("No alerts received.\nRun the rule engine, then inject a temperature fault."));
    list.setCellFactory(ignored -> new ListCell<>() {
      @Override
      protected void updateItem(DashboardModel.Alert alert, boolean empty) {
        super.updateItem(alert, empty);
        setText(null);
        if (empty || alert == null) {
          setGraphic(null);
          return;
        }
        var severity = new Label(alert.severity());
        severity.getStyleClass().addAll("status-pill", "alert-" + alert.severity().toLowerCase(Locale.ROOT));
        severity.setMinWidth(104);
        String description = alert.rule().equals("device-offline") ? "Device disconnected"
            : alert.severity().equals("RESOLVED") ? "Temperature back within range" : "High-temperature excursion";
        var name = new Label(alert.device() + "  ·  " + alert.site());
        name.getStyleClass().add("alert-device");
        var detail = new Label(
            description + (alert.value() == null ? "" : String.format(Locale.ROOT, "  ·  %.1f °C", alert.value())));
        detail.getStyleClass().add("muted");
        var text = new VBox(8, name, detail);
        var spacer = new Region();
        var time = new Label(TIME.format(Instant.ofEpochMilli(alert.timestamp())));
        time.getStyleClass().add("muted");
        var row = new HBox(24, severity, text, spacer, time);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(16));
        HBox.setHgrow(spacer, Priority.ALWAYS);
        setGraphic(row);
      }
    });
    var note = new Label(
        "Newest first. The monitor keeps the latest 1,000 alerts; the rule engine archives alerts to CSV.");
    note.getStyleClass().add("muted");
    var screen = new VBox(16, list, note);
    VBox.setVgrow(list, Priority.ALWAYS);
    return screen;
  }

  private static VBox metric(String label, Label value, String description) {
    var caption = new Label(label);
    caption.getStyleClass().add("metric-caption");
    value.getStyleClass().add("metric-value");
    var detail = new Label(description);
    detail.getStyleClass().add("muted");
    detail.setWrapText(true);
    var card = new VBox(8, caption, value, detail);
    card.getStyleClass().add("metric-card");
    card.setMaxWidth(Double.MAX_VALUE);
    return card;
  }

  private static <T> TableColumn<DashboardModel.Device, T> column(String label,
      Function<DashboardModel.Device, T> getter) {
    var column = new TableColumn<DashboardModel.Device, T>(label);
    column.setCellValueFactory(data -> new ReadOnlyObjectWrapper<>(getter.apply(data.getValue())));
    column.setMinWidth(96);
    return column;
  }

  private void connect() {
    reconnect.setDisable(true);
    connection.setText("Connecting…");
    Map<String, String> args = getParameters().getNamed();
    workers.submit(() -> {
      try {
        client.connect(new ConnectOptions(args.getOrDefault("host", "127.0.0.1"),
            Integer.parseInt(args.getOrDefault("port", "1883")),
            args.getOrDefault("client-id", "dashboard-" + UUID.randomUUID()),
            args.getOrDefault("username", "viewer"),
            System.getenv().getOrDefault("FROSTMQ_PASSWORD", "frostmq-demo").getBytes(StandardCharsets.UTF_8),
            30, null, Tls.clientFromEnvironment()));
        if (client.subscribe("coldchain/#", 1,
            (topic, payload, qos, retained) -> Platform.runLater(() -> update(topic, payload))) == 128)
          throw new MqttException("Viewer subscription denied");
        Platform.runLater(() -> {
          if (!closing) {
            connection.setText("Live · MQTT connected");
            reconnect.setText("Connected");
          }
        });
      } catch (Exception e) {
        client.disconnect();
        Platform.runLater(() -> {
          if (!closing) {
            connection.setText("Cannot connect · check broker and credentials");
            reconnect.setDisable(false);
            reconnect.setText("Retry");
          }
        });
      }
    });
  }

  private void update(String topic, byte[] payload) {
    if (closing || !model.apply(topic, payload))
      return;
    devices.setAll(model.devices());
    alerts.setAll(model.alerts());
    deviceCount.setText(Integer.toString(devices.size()));
    onlineCount.setText(Long.toString(model.onlineCount()));
    criticalCount.setText(Long.toString(model.criticalCount()));
  }

  @Override
  public void stop() {
    closing = true;
    Thread.ofVirtual().start(client::disconnect);
    workers.shutdownNow();
  }

  public static void main(String[] args) {
    launch(args);
  }
}
