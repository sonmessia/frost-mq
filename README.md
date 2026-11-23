# FrostMQ

A Java 25 MQTT 3.1.1 subset broker, blocking client, and cold-chain monitoring demo, built from `frostmq_architecture_spec_for_ai_generators.md`.

## Build

Install JDK 25 and Maven 3.9+. Run commands from this directory:

```sh
mvn -B install
```

This compiles all five modules, runs JUnit 5 tests, and installs the artifacts needed by the Maven launch commands below. The broker, common protocol, and client have no third-party runtime dependencies. JUnit is test-only. The applications use Gson 2.11.0; the dashboard also uses OpenJFX 25.

## Run the cold-chain demo

Open four terminals in this directory. Start the broker first, then the rule engine, simulator, and dashboard:

```sh
# Terminal 1
java -jar mqtt-broker/target/mqtt-broker-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

```sh
# Terminal 2
mvn -pl demo-apps exec:java -Dexec.mainClass=io.frostmq.demo.RuleEngine
```

```sh
# Terminal 3
mvn -pl demo-apps exec:java -Dexec.mainClass=io.frostmq.demo.DeviceSimulator \
  -Dexec.args="--nodes 5 --fault sensor01 --fault-after-seconds 6"
```

```sh
# Terminal 4, requires a graphical desktop
mvn -pl dashboard javafx:run
```

The simulator publishes every two seconds. `sensor01` and `sensor02` each include a cooling actuator listening to their own command topic. The fault raises `sensor01` to 9.5 °C. The engine emits a `CRITICAL` alert above 8 °C, turns cooling on, and emits `RESOLVED` with cooling off below 6 °C. The dashboard has device-fleet and alert-stream screens. Alerts also append to `alerts.csv`.

Ctrl+C stops an application. On normal simulator shutdown, nodes explicitly publish retained `offline` status and disconnect. On an unexpected socket loss or missed keep-alive, the broker publishes their Last Will. Duplicate client IDs disconnect the old session without its Will, as required by the blueprint.

### Options

The broker accepts positional arguments:

```sh
java -jar mqtt-broker/target/mqtt-broker-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  1883 users.json acl.json 127.0.0.1
```

The default bind address is loopback. Port `0` selects an ephemeral port for testing. TLS is optional in the blueprint and is not implemented here; do not expose this plaintext development broker to an untrusted network.

Simulator options use `--name value`: `--host`, `--port`, `--site`, `--nodes`, `--fault`, `--fault-after-seconds`, and `--duration-seconds`. Defaults are `127.0.0.1`, `1883`, `warehouse-a`, five nodes, no fault, six seconds before an enabled fault, and unlimited duration.

The engine accepts `--host`, `--port`, `--alerts`, and `--duration-seconds` (zero means unlimited). The dashboard accepts JavaFX named arguments, using `--name=value`:

```sh
mvn -pl dashboard javafx:run -Djavafx.args="--host=127.0.0.1 --port=1883"
```

All applications read `FROSTMQ_PASSWORD`, falling back to the public demo password below. The dashboard uses `viewer` unless `--username=...` is supplied. It can reconnect using its Retry/Reconnect button; there is no automatic reconnect or durable application queue.

## Credentials and ACLs

`users.json` contains public demonstration accounts: `sensor01` through `sensor10`, `engine`, and `viewer`. Their demo password is `frostmq-demo`. These are not production credentials. Change them before using real data.

Passwords are stored as Base64 PBKDF2-HMAC-SHA256 hashes with 210,000 iterations, 16-byte per-account salts, and 256-bit derived keys. Authentication uses constant-time hash comparison. `acl.json` is the policy from the blueprint. Device usernames must equal client IDs; subscription authorization checks containment of the requested filter, not just a match against its text. Wills must also be authorized.

Add an account or replace its password using the JDK-only credential utility. It reads a hidden console prompt when possible, or `FROSTMQ_PASSWORD` when set:

```sh
java -cp mqtt-broker/target/mqtt-broker-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  io.frostmq.broker.CredentialsTool users.json sensor11 device
```

The tool generates a random salt and atomically replaces the credentials file, with owner-only permissions on POSIX systems. Restart the broker to reload users or ACLs. Provision additional sensor accounts before running more than ten simulated nodes.

## Client API

```java
try (MqttClient client = new BlockingMqttClient()) {
    client.connect(new ConnectOptions("127.0.0.1", 1883, "my-viewer", "viewer", password));
    int granted = client.subscribe("coldchain/#", 1, (topic, payload, qos, retained) -> {
        // Reader virtual thread: hand off slow work to your own worker.
    });
    // granted is 0, 1, or 128 (ACL denial).
}
```

Classes are under `io.frostmq.client`; packet classes are under `io.frostmq.common`. `connect()` waits up to ten seconds, and QoS 1 publish, subscribe, and unsubscribe wait up to five seconds. A timeout closes the connection and fails pending requests. The client sends keep-alive pings automatically. Reconnect is explicit and starts a fresh clean session.

Callbacks run on the client reader virtual thread. Blocking QoS 1 publishes, subscriptions, and unsubscriptions from that thread fail fast rather than deadlock. The rule engine uses a separate virtual-thread worker; dashboard updates pass through `Platform.runLater`.

## Protocol and limits

- CONNECT must be the first packet and complete within ten seconds. Only protocol name `MQTT`, level 4, and Clean Session 1 are accepted. Empty non-device client IDs receive generated UUIDs.
- QoS 0 and 1, retained messages, Last Wills, wildcard subscriptions, unsubscribe, and heartbeat packets are supported. QoS 2 PUBLISH and QoS 2 handshake packets close the connection; QoS 2 subscription requests are downgraded to 1.
- Remaining Length is capped at 262,144 bytes, including the variable header and payload, matching the blueprint's decoder. Malformed UTF-8, invalid fixed flags, zero packet identifiers, malformed topics, and truncated frames are rejected.
- Each connection has one reader and one writer virtual thread. Publishers use nonblocking queue offers rather than writing subscriber sockets. Each outbound queue holds 1,000 packets: overflowing QoS 0 is dropped and counted; overflowing QoS 1 closes the subscriber.
- QoS 1 deliveries use per-session identifiers, tracked acknowledgements, and five-second duplicate retries. Unacknowledged deliveries are also capped at 1,000 to bound memory.
- Keep-alive expiry is checked every 100 ms against the 1.5× deadline. Keep-alive zero disables it after CONNECT.
- Unauthorized PUBLISH messages are dropped and logged; the sixth consecutive violation closes the socket. An authorized publish resets the counter. Unauthorized subscriptions receive SUBACK `0x80`.
- Overlapping subscriptions are coalesced to one delivery at the highest granted QoS. Live deliveries have RETAIN false; retained subscription replay has RETAIN true. An empty retained payload deletes the stored value.

This is an in-memory subset, not a production broker: no persistent sessions, QoS 2, WebSockets, TLS, clustering, or retained-store disk persistence. The subscription index uses a read/write-locked filter map rather than a specialized trie. Broker queues are bounded, but the total connection count and retained topic count are not capped. The engine's worker queue holds 10,000 events and logs overflow; its CSV output is local to the process.

## Tests

```sh
mvn -B verify
```

Tests cover wire-length boundaries (127, 128, 16,383, 16,384, and the packet cap), fragmented reads, malformed frames, wildcard and `$SYS` rules, PBKDF2/ACLs, protocol edge cases, queue overflow, QoS 1 retries, concurrent publishes, client timeouts, and the simulator-to-engine cooling/recovery path. Dashboard model tests cover device and alert projections.

For an independent standard-client check:

```sh
python3 -m venv .venv
.venv/bin/pip install paho-mqtt==2.1.0
.venv/bin/python scripts/paho_interop.py
```

The script starts its own loopback broker on an ephemeral port and checks Paho MQTT 3.1.1 authentication, ACLs, QoS 0/1, retained replay/deletion, commands, and keep-alive.

To test the real application launch commands and both JavaFX screens on a graphical desktop:

```sh
python3 scripts/demo_smoke.py
```

This starts an isolated broker, engine, and two-node simulator, runs the opt-in JavaFX test, checks CSV alerts, and saves fleet/alert screenshots under `artifacts/`. The normal Maven suite skips this GUI test so headless builds still work.

If Mosquitto tools are installed, these commands work against the running broker:

```sh
mosquitto_sub -h 127.0.0.1 -p 1883 -V mqttv311 \
  -u viewer -P frostmq-demo -t 'coldchain/#' -q 1 -v
mosquitto_pub -h 127.0.0.1 -p 1883 -V mqttv311 \
  -i sensor01 -u sensor01 -P frostmq-demo \
  -t coldchain/warehouse-a/sensor01/telemetry -q 1 \
  -m '{"temp":9.1,"hum":78.5,"door":0,"ts":1764000000123}'
```

Do not run the publisher while the simulator's `sensor01` is connected unless you intend to replace that session.
