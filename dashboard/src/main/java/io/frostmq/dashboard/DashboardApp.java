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
import java.util.function.Function;

public final class DashboardApp extends Application {
  private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss")
      .withZone(ZoneId.systemDefault());
  private final DashboardModel model = new DashboardModel();
  private final ObservableList<DashboardModel.Device> devices = FXCollections.observableArrayList();
  private final ObservableList<DashboardModel.Alert> alerts = FXCollections.observableArrayList();
  private final Label connection = new Label("Disconnected");
  private final Label deviceCount = new Label("0");
  private final Label onlineCount = new Label("0");
  private final Label criticalCount = new Label("0");
  private final Label alertCount = new Label("0");
  private final Label conditionTitle = new Label("Waiting for telemetry");
  private final Label conditionDetail = new Label("Start the simulator to establish a fleet baseline.");
  private final Label title = new Label("Fleet overview");
  private final Label subtitle = new Label("Live refrigeration conditions and device health");
  private final Button reconnect = new Button("Retry now");
  private final BorderPane content = new BorderPane();
  private final Region connectionSignal = new Region();
  private final HBox connectionBadge = new HBox(8, connectionSignal, connection);
  private volatile boolean closing;
  private volatile ReconnectingMqttSession feed;
  private HBox conditionPanel;
  private Node deviceScreen;
  private Node alertScreen;

  @Override
  public void start(Stage stage) {
    var root = new BorderPane();
    root.getStyleClass().add("app");
    root.setLeft(sidebar());
    var main = new VBox(22);
    main.setPadding(new Insets(32));
    title.getStyleClass().add("page-title");
    subtitle.getStyleClass().add("muted");
    var heading = new VBox(8, title, subtitle);
    connectionSignal.getStyleClass().add("connection-signal");
    connectionBadge.getStyleClass().addAll("connection-badge", "connection-offline");
    connectionBadge.setAlignment(Pos.CENTER_LEFT);
    var top = new HBox(16, heading, new Region(), connectionBadge, reconnect);
    top.getStyleClass().add("top-bar");
    HBox.setHgrow(top.getChildren().get(1), Priority.ALWAYS);
    top.setAlignment(Pos.CENTER_LEFT);
    connection.getStyleClass().add("connection-text");
    connection.setId("connection-status");
    reconnect.getStyleClass().add("primary-button");
    showRetry(false);
    reconnect.setOnAction(event -> {
      ReconnectingMqttSession current = feed;
      if (current != null)
        current.retryNow();
    });
    deviceScreen = devicesScreen();
    alertScreen = alertsScreen();
    content.setCenter(deviceScreen);
    VBox.setVgrow(content, Priority.ALWAYS);
    main.getChildren().addAll(top, content);
    root.setCenter(main);
    Scene scene = new Scene(root, 1280, 820);
    scene.getStylesheets().add(Objects.requireNonNull(getClass().getResource("dashboard.css")).toExternalForm());
    stage.setTitle("FrostMQ · Cold-chain monitor");
    stage.setMinWidth(960);
    stage.setMinHeight(600);
    stage.setScene(scene);
    stage.show();
    startFeed();
  }

  private Node sidebar() {
    var mark = new Label("F");
    mark.getStyleClass().add("brand-mark");
    var brand = new Label("FrostMQ");
    brand.getStyleClass().add("brand");
    var brandRow = new HBox(12, mark, brand);
    brandRow.getStyleClass().add("brand-row");
    brandRow.setAlignment(Pos.CENTER_LEFT);
    var caption = new Label("Cold-chain control");
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
      title.setText("Fleet overview");
      subtitle.setText("Live refrigeration conditions and device health");
    });
    log.setOnAction(event -> {
      log.setSelected(true);
      content.setCenter(alertScreen);
      title.setText("Incident ledger");
      subtitle.setText("Temperature excursions, recoveries, and connection loss");
    });
    var spacer = new Region();
    VBox.setVgrow(spacer, Priority.ALWAYS);
    var footer = new Label("MQTT 3.1.1\nClean-session telemetry\nJava 25 virtual threads");
    footer.getStyleClass().add("sidebar-footer");
    var side = new VBox(16, brandRow, caption, new Separator(), fleet, log, spacer, footer);
    side.setPadding(new Insets(32, 24, 32, 24));
    side.setPrefWidth(224);
    side.getStyleClass().add("sidebar");
    return side;
  }

  private Node devicesScreen() {
    conditionTitle.getStyleClass().add("condition-title");
    conditionTitle.setWrapText(true);
    conditionDetail.getStyleClass().add("condition-detail");
    conditionDetail.setWrapText(true);
    var conditionCopy = new VBox(7, new Label("Fleet condition"), conditionTitle, conditionDetail);
    conditionCopy.getChildren().getFirst().getStyleClass().add("condition-label");
    conditionCopy.getStyleClass().add("condition-copy");
    HBox.setHgrow(conditionCopy, Priority.ALWAYS);
    conditionPanel = new HBox(0, conditionCopy,
        operationalMetric("Devices seen", deviceCount, "retained in view"),
        operationalMetric("Reporting", onlineCount, "currently online"),
        operationalMetric("Above limit", criticalCount, "warmer than 8 °C"));
    conditionPanel.setAlignment(Pos.CENTER_LEFT);
    conditionPanel.getStyleClass().addAll("condition-panel", "condition-waiting");

    var table = new TableView<DashboardModel.Device>(devices);
    table.setId("device-table");
    table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
    table
        .setPlaceholder(new Label("No devices are reporting yet.\nStart DeviceSimulator to populate the live roster."));
    var site = column("Site", DashboardModel.Device::site);
    site.setPrefWidth(132);
    table.getColumns().add(site);
    var device = column("Device", DashboardModel.Device::id);
    device.setPrefWidth(112);
    table.getColumns().add(device);
    var status = column("Status", DashboardModel.Device::status);
    status.setPrefWidth(112);
    status.setCellFactory(ignored -> new TableCell<>() {
      @Override
      protected void updateItem(String value, boolean empty) {
        super.updateItem(value, empty);
        setText(null);
        if (empty || value == null) {
          setGraphic(null);
          return;
        }
        var dot = new Region();
        dot.getStyleClass().addAll("status-dot", "status-" + value);
        var label = new Label(value.substring(0, 1).toUpperCase(Locale.ROOT) + value.substring(1));
        label.getStyleClass().add("status-text");
        var indicator = new HBox(7, dot, label);
        indicator.setAlignment(Pos.CENTER_LEFT);
        setGraphic(indicator);
      }
    });
    table.getColumns().add(status);
    var temperature = column("Temperature", DashboardModel.Device::temperature);
    temperature.setPrefWidth(112);
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
    table.getColumns().add(column("Humidity",
        d -> d.humidity() == null ? "—" : String.format(Locale.ROOT, "%.1f %%", d.humidity())));
    table.getColumns().add(column("Door", d -> d.door() == null ? "—" : d.door() == 1 ? "Open" : "Closed"));
    table.getColumns().add(column("Updated",
        d -> d.updated() == 0 ? "—" : TIME.format(Instant.ofEpochMilli(d.updated()))));
    var section = new Label("Live device roster");
    section.getStyleClass().add("section-title");
    var note = new Label("Target 4 °C     Alert above 8 °C     Recovery below 6 °C");
    note.getStyleClass().add("muted");
    var screen = new VBox(16, conditionPanel, section, table, note);
    VBox.setVgrow(table, Priority.ALWAYS);
    return screen;
  }

  private Node alertsScreen() {
    alertCount.getStyleClass().add("archive-count");
    var archiveCopy = new VBox(5, new Label("Incident archive"),
        new Label("Operational events received during this dashboard session."));
    archiveCopy.getChildren().getFirst().getStyleClass().add("archive-title");
    archiveCopy.getChildren().getLast().getStyleClass().add("muted-on-dark");
    HBox.setHgrow(archiveCopy, Priority.ALWAYS);
    var archiveMetric = new VBox(3, alertCount, new Label("events retained"));
    archiveMetric.getStyleClass().add("archive-metric");
    archiveMetric.getChildren().getLast().getStyleClass().add("muted-on-dark");
    var archive = new HBox(24, archiveCopy, archiveMetric);
    archive.setAlignment(Pos.CENTER_LEFT);
    archive.getStyleClass().add("archive-strip");

    var list = new ListView<DashboardModel.Alert>(alerts);
    list.setId("alert-list");
    list.setPlaceholder(new Label("No incidents recorded.\nRun the rule engine and inject a temperature fault."));
    list.setCellFactory(ignored -> new ListCell<>() {
      @Override
      protected void updateItem(DashboardModel.Alert alert, boolean empty) {
        super.updateItem(alert, empty);
        setText(null);
        if (empty || alert == null) {
          setGraphic(null);
          return;
        }
        String level = alert.severity().toLowerCase(Locale.ROOT);
        var rail = new Region();
        rail.getStyleClass().addAll("alert-rail", "rail-" + level);
        var severity = new Label(alert.severity());
        severity.getStyleClass().addAll("severity-label", "severity-" + level);
        severity.setMinWidth(92);
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
        var row = new HBox(18, rail, severity, text, spacer, time);
        row.getStyleClass().add("alert-row");
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(16));
        HBox.setHgrow(spacer, Priority.ALWAYS);
        setGraphic(row);
      }
    });
    var section = new Label("Recent incidents");
    section.getStyleClass().add("section-title");
    var note = new Label("Newest first. Up to 1,000 events stay in memory; the rule engine writes the CSV archive.");
    note.getStyleClass().add("muted");
    var screen = new VBox(16, archive, section, list, note);
    VBox.setVgrow(list, Priority.ALWAYS);
    return screen;
  }

  private static VBox operationalMetric(String label, Label value, String description) {
    var caption = new Label(label);
    caption.getStyleClass().add("operational-label");
    value.getStyleClass().add("operational-value");
    var detail = new Label(description);
    detail.getStyleClass().add("operational-detail");
    var metric = new VBox(4, caption, value, detail);
    metric.getStyleClass().add("operational-metric");
    return metric;
  }

  private static <T> TableColumn<DashboardModel.Device, T> column(String label,
      Function<DashboardModel.Device, T> getter) {
    var column = new TableColumn<DashboardModel.Device, T>(label);
    column.setCellValueFactory(data -> new ReadOnlyObjectWrapper<>(getter.apply(data.getValue())));
    column.setMinWidth(96);
    return column;
  }

  private void startFeed() {
    Map<String, String> args = getParameters().getNamed();
    try {
      var options = new ConnectOptions(args.getOrDefault("host", "127.0.0.1"),
          Integer.parseInt(args.getOrDefault("port", "1883")),
          args.getOrDefault("client-id", "dashboard-" + UUID.randomUUID()),
          args.getOrDefault("username", "viewer"),
          System.getenv().getOrDefault("FROSTMQ_PASSWORD", "frostmq-demo").getBytes(StandardCharsets.UTF_8),
          30, null, Tls.clientFromEnvironment());
      feed = new ReconnectingMqttSession(new BlockingMqttClient(), options,
          mqtt -> {
            if (mqtt.subscribe("coldchain/#", 1,
                (topic, payload, qos, retained) -> Platform.runLater(() -> update(topic, payload))) == 128)
              throw new MqttException("Viewer subscription denied");
          },
          state -> Platform.runLater(() -> showConnectionState(state)));
      feed.start();
    } catch (Exception e) {
      connection.setText("Invalid connection settings");
      showRetry(false);
    }
  }

  private void showConnectionState(ReconnectingMqttSession.State state) {
    if (closing)
      return;
    switch (state.status()) {
      case CONNECTING -> {
        setConnectionClass("connection-connecting");
        connection.setText("Connecting to broker");
        showRetry(false);
      }
      case CONNECTED -> {
        setConnectionClass("connection-live");
        connection.setText("Live MQTT feed");
        showRetry(false);
      }
      case RETRYING -> {
        setConnectionClass("connection-lost");
        connection.setText("Connection lost; retrying");
        showRetry(true);
      }
    }
  }

  private void setConnectionClass(String styleClass) {
    connectionBadge.getStyleClass().removeAll(
        "connection-offline", "connection-connecting", "connection-live", "connection-lost");
    connectionBadge.getStyleClass().add(styleClass);
  }

  private void showRetry(boolean visible) {
    reconnect.setManaged(visible);
    reconnect.setVisible(visible);
    reconnect.setDisable(!visible);
  }

  private void update(String topic, byte[] payload) {
    if (closing || !model.apply(topic, payload))
      return;
    devices.setAll(model.devices());
    alerts.setAll(model.alerts());
    deviceCount.setText(Integer.toString(devices.size()));
    onlineCount.setText(Long.toString(model.onlineCount()));
    criticalCount.setText(Long.toString(model.criticalCount()));
    alertCount.setText(Integer.toString(alerts.size()));
    updateFleetCondition();
  }

  private void updateFleetCondition() {
    conditionPanel.getStyleClass().removeAll(
        "condition-waiting", "condition-safe", "condition-warning", "condition-critical");
    long seen = devices.size(), online = model.onlineCount(), critical = model.criticalCount();
    if (seen == 0) {
      conditionPanel.getStyleClass().add("condition-waiting");
      conditionTitle.setText("Waiting for telemetry");
      conditionDetail.setText("Start the simulator to establish a fleet baseline.");
    } else if (critical > 0) {
      conditionPanel.getStyleClass().add("condition-critical");
      conditionTitle.setText(critical == 1 ? "Temperature excursion active" : "Temperature excursions active");
      conditionDetail.setText(critical + (critical == 1 ? " device is" : " devices are")
          + " above the 8 °C intervention threshold.");
    } else if (online < seen) {
      conditionPanel.getStyleClass().add("condition-warning");
      conditionTitle.setText("Telemetry coverage interrupted");
      conditionDetail.setText((seen - online) + (seen - online == 1 ? " device is" : " devices are")
          + " offline or awaiting a fresh status update.");
    } else {
      conditionPanel.getStyleClass().add("condition-safe");
      conditionTitle.setText("All monitored devices are in range");
      conditionDetail.setText("Every registered sensor is online and below the alert threshold.");
    }
  }

  @Override
  public void stop() {
    closing = true;
    ReconnectingMqttSession current = feed;
    if (current != null)
      Thread.ofVirtual().start(current::close);
  }

  public static void main(String[] args) {
    launch(args);
  }
}
