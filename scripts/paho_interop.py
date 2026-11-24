#!/usr/bin/env python3
"""Independent MQTT 3.1.1 interoperability check. Requires paho-mqtt==2.1.0."""

import os
from pathlib import Path
import queue
import selectors
import subprocess
import threading
import time
import uuid

import paho.mqtt.client as mqtt

ROOT = Path(__file__).resolve().parents[1]
PASSWORD = os.environ.get("FROSTMQ_PASSWORD", "frostmq-demo")


class Peer:
    def __init__(self, port, username, client_id=None, password=PASSWORD, will=None):
        self.messages = queue.Queue()
        self.connected = threading.Event()
        self.connect_code = None
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id or str(uuid.uuid4()),
            clean_session=True,
            protocol=mqtt.MQTTv311,
        )
        self.client.username_pw_set(username, password)
        if will:
            self.client.will_set(will, b"offline", qos=1, retain=True)
        self.client.on_connect = self.on_connect
        self.client.on_message = lambda client, userdata, message: self.messages.put(
            (message.topic, message.payload, message.qos, message.retain)
        )
        self.subscriptions = queue.Queue()
        self.client.on_subscribe = (
            lambda client, userdata, mid, codes, properties: self.subscriptions.put(
                [code.value for code in codes]
            )
        )
        self.client.reconnect_delay_set(30, 30)
        self.client.connect("127.0.0.1", port, keepalive=2)
        self.client.loop_start()
        assert self.connected.wait(5), "CONNACK timed out"

    def on_connect(self, client, userdata, flags, reason_code, properties):
        self.connect_code = reason_code.value
        self.connected.set()

    def subscribe(self, topic, qos=1):
        self.client.subscribe(topic, qos)
        return self.subscriptions.get(timeout=3)

    def publish(self, topic, payload, qos=1, retain=False):
        info = self.client.publish(topic, payload, qos, retain)
        info.wait_for_publish(timeout=3)
        assert info.is_published(), "PUBACK timed out"

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


def main():
    jar = (
        ROOT / "mqtt-broker/target/mqtt-broker-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
    )
    process = subprocess.Popen(
        [
            "java",
            "-jar",
            str(jar),
            "0",
            str(ROOT / "users.json"),
            str(ROOT / "acl.json"),
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    peers = []
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=10), "Broker did not start"
            line = process.stdout.readline().strip()
        assert line.startswith("FrostMQ listening on "), line
        port = int(line.rsplit(":", 1)[1])

        def peer(*args, **kwargs):
            item = Peer(port, *args, **kwargs)
            peers.append(item)
            return item

        viewer = peer("viewer")
        assert viewer.connect_code == 0
        assert viewer.subscribe("coldchain/#") == [1]
        device = peer("sensor01", client_id="sensor01")
        assert device.connect_code == 0
        takeover = peer("viewer", client_id="sensor01")
        assert (
            takeover.connect_code == 133
        )  # Another account cannot take the device ID.
        assert device.subscribe("coldchain/+/sensor01/cmd", qos=2) == [1]
        assert device.subscribe("coldchain/+/+/cmd") == [128]
        assert device.subscribe("coldchain/#") == [128]
        topic = "coldchain/interop/sensor01/telemetry"
        payload = b'{"temp":4.2,"hum":78.5,"door":0,"ts":1764000000123}'
        device.publish(topic, payload, retain=True)
        assert viewer.messages.get(timeout=3) == (topic, payload, 1, False)

        replay = peer("viewer")
        assert replay.subscribe(topic, qos=0) == [0]
        assert replay.messages.get(timeout=3) == (topic, payload, 0, True)
        device.publish(topic, b"", retain=True)
        assert viewer.messages.get(timeout=3) == (topic, b"", 1, False)
        assert replay.messages.get(timeout=3) == (topic, b"", 0, False)
        deleted = peer("viewer")
        assert deleted.subscribe(topic) == [1]
        try:
            deleted.messages.get(timeout=0.2)
            raise AssertionError("Deleted retained message was replayed")
        except queue.Empty:
            pass

        engine = peer("engine")
        engine.publish("coldchain/interop/sensor01/cmd", b'{"power":"on"}')
        assert device.messages.get(timeout=3)[1] == b'{"power":"on"}'
        assert viewer.messages.get(timeout=3)[0].endswith("/cmd")
        denied = peer("viewer", password="incorrect")
        assert (
            denied.connect_code == 134
        )  # Paho maps MQTT 3.1.1 code 4 to MQTT 5 reason 134.
        mismatch = peer("sensor02", client_id="wrong-id")
        assert mismatch.connect_code == 133
        print(
            "PASS: Paho MQTT 3.1.1 auth, ACL containment, QoS 0/1, QoS 2 subscription downgrade, retained replay/deletion, and commands"
        )
        time.sleep(3)  # Exercise Paho's keep-alive, beyond the broker's 1.5x deadline.
        engine.publish(
            "coldchain/alerts/WARNING", b'{"rule":"interop","device":"sensor01","ts":1}'
        )
        assert viewer.messages.get(timeout=3)[0] == "coldchain/alerts/WARNING"
        print("PASS: Paho keep-alive")
    finally:
        for item in peers:
            item.close()
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        errors = process.stderr.read()
        if process.returncode not in (0, 143, -15):
            raise RuntimeError(f"Broker exit {process.returncode}: {errors}")


if __name__ == "__main__":
    main()
