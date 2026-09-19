"""Run a disposable three-broker/two-worker lab; requires local Docker, no AWS."""

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "data/resilience-evidence"
COMPOSE = [
    "docker",
    "compose",
    "-p",
    "merchmind-resilience",
    "-f",
    str(ROOT / "docker-compose.resilience.yml"),
]


def compose(*args, **kwargs):
    return subprocess.run([*COMPOSE, *args], cwd=ROOT, check=True, **kwargs)


def probe(*args):
    compose("exec", "-T", "driver", "python3", "scripts/cluster_probe.py", *args)


def spark(stage):
    compose(
        "exec",
        "-T",
        "driver",
        "python3",
        "scripts/run_stream.py",
        "--bootstrap-servers",
        "broker-a:9092,broker-b:9092,broker-c:9092",
        "--topic",
        "resilience-transactions",
        "--output",
        "/app/evidence/output",
        "--checkpoint",
        "/app/evidence/checkpoints",
        "--available-now",
        "--progress-report",
        f"/app/evidence/{stage}-progress.json",
        timeout=300,
    )


def main():
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    if (EVIDENCE / "checkpoints").exists():
        raise SystemExit("Archive data/resilience-evidence before running a fresh lab.")
    existing = compose("ps", "--all", "--quiet", capture_output=True, text=True)
    if existing.stdout.strip():
        raise SystemExit("An existing merchmind-resilience stack is present; inspect it first.")
    try:
        compose("up", "-d", "--build", "--wait", timeout=600)
        probe("create")
        probe("produce")
        leader = json.loads((EVIDENCE / "leader.json").read_text())["leader"]
        failed = {0: "broker-a", 1: "broker-b", 2: "broker-c"}[leader]
        compose("kill", "-s", "SIGKILL", failed)
        probe("produce", "--start", "10000")
        spark("degraded")  # Reads replicated backlog with the leader still down.
        compose("start", failed)
        probe("produce", "--start", "20000")
        probe("produce", "--start", "30000", "--count", "1", "--marker")
        spark("restored")
        probe("verify")
    finally:
        with (EVIDENCE / "containers.log").open("w") as log:
            subprocess.run(
                [*COMPOSE, "logs", "--no-color"], stdout=log, stderr=subprocess.STDOUT, timeout=60
            )
        compose("down", "--volumes", "--remove-orphans", timeout=90)


if __name__ == "__main__":
    main()
