#!/usr/bin/env python3
"""Run the real application mains and opt-in JavaFX UI test; needs a graphical display."""
import csv
import os
from pathlib import Path
import selectors
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"


def ready(process, text, timeout=15):
    deadline = time.monotonic() + timeout
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while time.monotonic() < deadline:
            if not selector.select(timeout=max(0, deadline - time.monotonic())):
                break
            line = process.stdout.readline().decode("utf-8", errors="replace")
            if not line:
                raise RuntimeError(f"Application exited before readiness: {process.args}")
            if text in line:
                return line.strip()
    raise TimeoutError(f"Application startup timed out: {process.args}")


def main():
    ARTIFACTS.mkdir(exist_ok=True)
    processes = []

    def launch(command):
        process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, bufsize=0)
        processes.append(process)
        return process

    try:
        broker = launch(["java", "-jar", str(ROOT / "mqtt-broker/target/mqtt-broker-1.0.0-SNAPSHOT-jar-with-dependencies.jar"), "0"])
        port = ready(broker, "FrostMQ listening on").rsplit(":", 1)[1]
        csv_path = ARTIFACTS / "demo-alerts.csv"
        engine = launch(["mvn", "-B", "-pl", "demo-apps", "exec:java",
                         "-Dexec.mainClass=io.frostmq.demo.RuleEngine",
                         f"-Dexec.args=--port {port} --duration-seconds 30 --alerts {csv_path}"])
        ready(engine, "Rule engine ready.")
        simulator = launch(["mvn", "-B", "-pl", "demo-apps", "exec:java",
                            "-Dexec.mainClass=io.frostmq.demo.DeviceSimulator",
                            f"-Dexec.args=--port {port} --nodes 2 --fault sensor01 --fault-after-seconds 8 --duration-seconds 22"])
        ready(simulator, "Sensors online.")
        environment = dict(os.environ, FROSTMQ_GUI_TEST="1", FROSTMQ_GUI_PORT=port)
        result = subprocess.run(["mvn", "-B", "-pl", "dashboard", "-Dtest=DashboardUiTest", "test"],
                                cwd=ROOT, env=environment, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=35)
        (ARTIFACTS / "gui-test.log").write_text(result.stdout)
        if result.returncode:
            raise RuntimeError("JavaFX UI test failed; see artifacts/gui-test.log")
        simulator_output, _ = simulator.communicate(timeout=25)
        assert simulator.returncode == 0, simulator_output
        engine_output, _ = engine.communicate(timeout=15)
        assert engine.returncode == 0, engine_output
        with csv_path.open() as source:
            rows = list(csv.DictReader(source))
        assert any(row["severity"] == "CRITICAL" for row in rows)
        assert any(row["severity"] == "RESOLVED" for row in rows)
        assert any(row["severity"] == "WARNING" for row in rows)
        print("PASS: real broker, engine and simulator launch commands; cooling/recovery/offline alerts; CSV")
        print("PASS: real JavaFX fleet, alert navigation and rendering")
        print(f"Screenshots: {ARTIFACTS / 'dashboard-fleet.png'}, {ARTIFACTS / 'dashboard-alerts.png'}")
    finally:
        for process in reversed(processes):
            if process.returncode is None:
                process.terminate()
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=5)


if __name__ == "__main__":
    main()
