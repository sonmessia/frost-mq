# FrostMQ

FrostMQ is a Java 25 implementation of a focused MQTT 3.1.1 broker and blocking client. The repository also includes a cold-chain demo with simulated sensors, an alerting rule engine, and a JavaFX dashboard.

This project is intended for local development, protocol experiments, and demonstrations. It is not a production MQTT broker.

## Repository modules

| Module | Purpose |
| --- | --- |
| `mqtt-common` | MQTT packets, codec, topic validation, TLS helpers |
| `mqtt-broker` | In-memory MQTT broker, authentication, ACLs, quotas |
| `mqtt-client` | Blocking client and automatic reconnect supervision |
| `demo-apps` | Cold-chain rule engine and device simulator |
| `dashboard` | JavaFX fleet and alert dashboard |

## Requirements

- JDK 25
- Maven 3.9 or newer
- A graphical desktop only when running the JavaFX dashboard
- Python 3 only for the optional validation scripts

Confirm that the correct tools are active:

```sh
java --version
mvn --version
```

All commands below must be run from the repository root.

## Quick start

### 1. Build the project

```sh
mvn -B clean install
```

This compiles all modules, runs the JUnit suite, installs the local artifacts required by the Maven launch commands, and creates the executable broker JAR:

```text
mqtt-broker/target/mqtt-broker-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

### 2. Start the complete demo

Open four terminals in the repository root. Start the processes in the following order.

**Terminal 1 — broker**

```sh
java -jar mqtt-broker/target/mqtt-broker-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

Expected output:

```text
FrostMQ listening on 127.0.0.1:1883
```

**Terminal 2 — rule engine**

```sh
mvn -pl demo-apps exec:java \
  -Dexec.mainClass=io.frostmq.demo.RuleEngine
```

**Terminal 3 — simulated devices**

```sh
mvn -pl demo-apps exec:java \
  -Dexec.mainClass=io.frostmq.demo.DeviceSimulator \
  -Dexec.args="--nodes 5 --fault sensor01 --fault-after-seconds 6"
```

**Terminal 4 — JavaFX dashboard**

```sh
mvn -pl dashboard javafx:run
```

The dashboard requires a graphical desktop. For a headless demo, run only the first three processes and use the Mosquitto subscriber shown in [Use external MQTT tools](#use-external-mqtt-tools).

### 3. Observe the demo

- Sensors publish retained telemetry every two seconds.
- `sensor01` and `sensor02` have simulated cooling actuators.
- After six seconds, the example command raises `sensor01` to 9.5 °C.
- The rule engine emits a `CRITICAL` alert above 8 °C and turns cooling on.
- When the temperature drops below 6 °C, it emits `RESOLVED` and turns cooling off.
- The dashboard shows the current device fleet and alert stream.
- Alerts are appended to `alerts.csv` in the repository root.

Press `Ctrl+C` in each terminal to stop the applications. Stop the broker last if you want devices to publish their final retained `offline` status cleanly.

## Demo configuration

### Broker arguments

The broker accepts positional arguments in this order:

```sh
java -jar mqtt-broker/target/mqtt-broker-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  [port] [users-file] [acl-file] [bind-host]
```

Example:

```sh
java -jar mqtt-broker/target/mqtt-broker-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  1883 users.json acl.json 127.0.0.1
```

| Position | Default | Description |
| --- | --- | --- |
| `port` | `1883` | MQTT listener port; `0` selects an ephemeral test port |
| `users-file` | `users.json` | Account and password-hash configuration |
| `acl-file` | `acl.json` | Publish and subscribe ACL configuration |
| `bind-host` | `127.0.0.1` | Listener address |

The default listener is plaintext and bound to loopback. Do not expose it to an untrusted network. Use TLS before changing the bind address to a remotely reachable interface.

### Rule engine options

Rule engine options use `--name value` syntax.

| Option | Default | Description |
| --- | --- | --- |
| `--host` | `127.0.0.1` | Broker host |
| `--port` | `1883` | Broker port |
| `--alerts` | `alerts.csv` | CSV alert output path |
| `--duration-seconds` | `0` | Runtime; `0` means unlimited |

Example:

```sh
mvn -pl demo-apps exec:java \
  -Dexec.mainClass=io.frostmq.demo.RuleEngine \
  -Dexec.args="--host 127.0.0.1 --port 1883 --alerts alerts.csv --duration-seconds 60"
```

### Device simulator options

Simulator options also use `--name value` syntax.

| Option | Default | Description |
| --- | --- | --- |
| `--host` | `127.0.0.1` | Broker host |
| `--port` | `1883` | Broker port |
| `--site` | `warehouse-a` | Site topic level |
| `--nodes` | `5` | Number of simulated sensors |
| `--fault` | empty | Device that receives a temperature fault |
| `--fault-after-seconds` | `6` | Delay before fault injection |
| `--duration-seconds` | `0` | Runtime; `0` means unlimited |

The included credentials provision `sensor01` through `sensor10`. Add more sensor accounts before using `--nodes` with a value greater than 10.

### Dashboard options

JavaFX named arguments use `--name=value` syntax:

```sh
mvn -pl dashboard javafx:run \
  -Djavafx.args="--host=127.0.0.1 --port=1883 --username=viewer"
```

Supported arguments are `--host`, `--port`, `--username`, and `--client-id`. The defaults are `127.0.0.1`, `1883`, `viewer`, and a generated dashboard client ID.

### Environment variables

| Variable | Used by | Default |
| --- | --- | --- |
| `FROSTMQ_PASSWORD` | Rule engine, simulator, dashboard, credential tool | `frostmq-demo` for demo applications |
| `FROSTMQ_TLS_KEYSTORE` | Broker | TLS disabled |
| `FROSTMQ_TLS_KEYSTORE_PASSWORD` | Broker | Required when a keystore is configured |
| `FROSTMQ_TLS_TRUSTSTORE` | Clients and applications | TLS disabled |
| `FROSTMQ_TLS_TRUSTSTORE_PASSWORD` | Clients and applications | Required when a truststore is configured |
| `FROSTMQ_MAX_CONNECTIONS` | Broker | `1024` |
| `FROSTMQ_MAX_FILTERS_PER_SESSION` | Broker | `100` |
| `FROSTMQ_MAX_FILTERS` | Broker | `10000` |
| `FROSTMQ_MAX_RETAINED` | Broker | `10000` |

The rule engine, simulator, and dashboard reconnect automatically with capped exponential backoff. They recreate their clean sessions and subscriptions after reconnect. The dashboard's **Retry now** button bypasses the current delay. Messages published while an application is offline are not queued for later delivery.

## Enable TLS

FrostMQ supports optional server-authenticated TLS with PKCS12 stores. Mutual TLS is not implemented.

### 1. Create development key material

`keytool` is included with the JDK:

```sh
keytool -genkeypair -alias frostmq -keyalg RSA -keysize 2048 -validity 365 \
  -storetype PKCS12 -keystore server.p12 -storepass changeit \
  -dname "CN=localhost" -ext "SAN=IP:127.0.0.1,dns:localhost" -noprompt

keytool -exportcert -alias frostmq -keystore server.p12 \
  -storepass changeit -file server.crt

keytool -importcert -noprompt -alias frostmq -file server.crt \
  -keystore trust.p12 -storetype PKCS12 -storepass changeit
```

These passwords are examples only. Use protected secrets and appropriately managed certificates outside local development.

### 2. Start the TLS broker

```sh
FROSTMQ_TLS_KEYSTORE=server.p12 \
FROSTMQ_TLS_KEYSTORE_PASSWORD=changeit \
java -jar mqtt-broker/target/mqtt-broker-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

Expected output includes `(TLS)`:

```text
FrostMQ listening on 127.0.0.1:1883 (TLS)
```

### 3. Configure every client process

Set the truststore variables in each terminal that runs the rule engine, simulator, or dashboard:

```sh
export FROSTMQ_TLS_TRUSTSTORE="$PWD/trust.p12"
export FROSTMQ_TLS_TRUSTSTORE_PASSWORD=changeit
```

Then use the same launch commands from the quick-start section.

The requested host must match the certificate Subject Alternative Name (SAN):

- Connecting to `127.0.0.1` requires `SAN=IP:127.0.0.1`.
- Connecting to `localhost` requires `SAN=dns:localhost`.
- A certificate containing only a Common Name is rejected.
- An untrusted certificate or a SAN mismatch causes the connection to fail.

## Credentials and ACLs

The tracked `users.json` file contains demonstration accounts:

| Accounts | Role | Demo password |
| --- | --- | --- |
| `sensor01` through `sensor10` | `device` | `frostmq-demo` |
| `engine` | `engine` | `frostmq-demo` |
| `viewer` | `viewer` | `frostmq-demo` |

These credentials are public and must not be used with real data.

Device usernames and client IDs must be identical. Account usernames are reserved client IDs, including while an account is offline. A live client ID may only be replaced by another session authenticated as the same account; a cross-account takeover is rejected with CONNACK code 2.

Add an account or replace its password with the credential utility:

```sh
java -cp mqtt-broker/target/mqtt-broker-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  io.frostmq.broker.CredentialsTool users.json sensor11 device
```

The utility reads a hidden console prompt when available. If `FROSTMQ_PASSWORD` is set, it uses that value instead. It generates a random salt, writes PBKDF2-HMAC-SHA256 credentials atomically, and applies owner-only permissions on POSIX systems.

Restart the broker after changing `users.json` or `acl.json`.

## Use external MQTT tools

With Mosquitto clients installed, subscribe as the read-only viewer:

```sh
mosquitto_sub -h 127.0.0.1 -p 1883 -V mqttv311 \
  -u viewer -P frostmq-demo -t 'coldchain/#' -q 1 -v
```

Publish telemetry as `sensor10`:

```sh
mosquitto_pub -h 127.0.0.1 -p 1883 -V mqttv311 \
  -i sensor10 -u sensor10 -P frostmq-demo \
  -t coldchain/warehouse-a/sensor10/telemetry -q 1 \
  -m '{"temp":9.1,"hum":78.5,"door":0,"ts":1764000000123}'
```

Do not reuse a sensor client ID while that sensor is running unless you intend to replace its existing same-account session.

## Client usage

```java
try (MqttClient client = new BlockingMqttClient()) {
    client.connect(new ConnectOptions(
            "127.0.0.1", 1883, "my-viewer", "viewer", "frostmq-demo"));

    int grantedQos = client.subscribe("coldchain/#", 1,
            (topic, payload, qos, retained) -> {
                // This callback runs on the reader virtual thread.
                // Hand slow or blocking work to another executor.
            });

    if (grantedQos == 128) {
        throw new IllegalStateException("Subscription denied by the broker ACL");
    }
}
```

`BlockingMqttClient.connect()` has one absolute 10-second deadline shared by TCP, TLS, and CONNACK. QoS 1 publish, subscribe, and unsubscribe operations wait up to five seconds. Blocking MQTT operations invoked from a message callback fail fast rather than deadlock.

For long-running applications, `ReconnectingMqttSession` adds initial-connect recovery, capped backoff, session-restoration callbacks, and manual retry support around an `MqttClient`.

## Supported protocol and limits

- MQTT 3.1.1 (`MQTT`, protocol level 4), Clean Session only
- QoS 0 and QoS 1 publish and delivery
- QoS 2 subscription requests downgraded to QoS 1; QoS 2 publish is unsupported
- Retained messages and retained deletion with an empty payload
- Last Wills, keep-alive, wildcard subscriptions, and unsubscribe
- Exact, `+`, and `#` subscription trie matching, including MQTT `$SYS` isolation
- Maximum packet Remaining Length: 262,144 bytes
- Outbound queue per connection: 1,000 packets
- Unacknowledged QoS 1 deliveries per connection: 1,000
- Five-second QoS 1 duplicate retry interval

The broker is in-memory only. It does not provide persistent sessions, retained-message disk persistence, WebSockets, clustering, QoS 2 delivery, or durable application queues.

## Verification

Run the complete Maven test suite:

```sh
mvn -B verify
```

Run the independent Paho MQTT 3.1.1 interoperability check:

```sh
python3 -m venv .venv
.venv/bin/pip install paho-mqtt==2.1.0
.venv/bin/python scripts/paho_interop.py
```

Run the real-process and JavaFX smoke test on a graphical desktop:

```sh
python3 scripts/demo_smoke.py
```

The smoke test writes logs, CSV output, and dashboard screenshots under `artifacts/`. The regular Maven build skips the graphical UI test so that headless builds remain supported.

## Troubleshooting

| Symptom | Resolution |
| --- | --- |
| Broker JAR is missing | Run `mvn -B clean install` from the repository root |
| `release version 25 not supported` | Point `JAVA_HOME` and `PATH` to JDK 25 |
| `Connection refused` | Start the broker and verify that all processes use the same host and port |
| `Address already in use` | Stop the existing broker or choose another port for every process |
| CONNECT is rejected with code 2 | Use an unreserved client ID, or authenticate as the account that owns that ID |
| TLS handshake fails | Verify the truststore, password, requested host, and certificate SAN |
| Dashboard cannot open a display | Run it on a graphical desktop with a valid display session, or use headless mode |
| Nodes above `sensor10` cannot connect | Provision matching accounts with `CredentialsTool` before increasing `--nodes` |
| Alerts are not visible | Start the rule engine and simulator, then verify `alerts.csv` and the broker logs |
