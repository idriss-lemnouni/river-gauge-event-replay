"""Publish a checksummed USGS snapshot to the project's Docker Kafka broker."""
import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryFile
from time import perf_counter

from fetch_observations import canonical

HEX_KEY = re.compile(r"[0-9a-f]{64}")
KAFKA_TOOLS = "/opt/kafka/bin/"
BOOTSTRAP = "kafka:9092"


@contextmanager
def prepare_replay(source: Path, manifest: Path, replay_id: str):
    metadata = json.loads(manifest.read_text(encoding="utf-8"))
    expected_size = sum(station["records"] for station in metadata["stations"].values())
    digest = hashlib.sha256()
    count = 0
    with TemporaryFile() as messages, source.open("rb") as records:
        for line in records:
            digest.update(line)
            event = json.loads(line)
            key = event.get("event_key", "")
            if not HEX_KEY.fullmatch(key) or not HEX_KEY.fullmatch(event.get("record_key", "")):
                raise ValueError(f"Invalid observation identity at line {count + 1}")
            payload = canonical({"replay_id": replay_id, "event": event})
            messages.write(key.encode("ascii") + b"\t" + payload + b"\n")
            count += 1
        if digest.hexdigest() != metadata["sha256"] or count != expected_size or not count:
            raise ValueError("Snapshot record count or SHA-256 differs from its manifest")
        messages.seek(0)
        yield messages, count


def end_offsets(docker: list[str], topic: str) -> dict[int, int]:
    response = subprocess.run(docker + [
        KAFKA_TOOLS + "kafka-get-offsets.sh", "--bootstrap-server", BOOTSTRAP,
        "--topic", topic, "--time", "-1",
    ], check=True, capture_output=True, text=True)
    offsets = {}
    for line in response.stdout.splitlines():
        name, part, offset = line.rsplit(":", maxsplit=2)
        if name != topic or int(part) in offsets:
            raise ValueError("Kafka returned unexpected partition metadata")
        offsets[int(part)] = int(offset)
    if not offsets:
        raise ValueError("Kafka returned no partition offsets")
    return offsets


def publish(source: Path, manifest: Path, topic: str, replay_id: str) -> dict:
    docker = [
        "docker", "compose", "exec", "-T", "-e", "KAFKA_HEAP_OPTS=-Xms32m -Xmx160m", "kafka",
    ]
    with prepare_replay(source, manifest, replay_id) as (messages, count):
        subprocess.run(docker + [
            KAFKA_TOOLS + "kafka-topics.sh", "--bootstrap-server", BOOTSTRAP,
            "--create", "--if-not-exists", "--topic", topic, "--partitions", "2",
            "--replication-factor", "1", "--config", "message.timestamp.type=LogAppendTime",
        ], check=True, stdout=subprocess.DEVNULL)
        before = end_offsets(docker, topic)
        started = perf_counter()
        subprocess.run(docker + [
            KAFKA_TOOLS + "kafka-console-producer.sh", "--bootstrap-server", BOOTSTRAP,
            "--topic", topic, "--producer-property", "acks=all",
            "--producer-property", "enable.idempotence=true",
            "--producer-property", "linger.ms=10",
            "--property", "parse.key=true",
        ], check=True, stdin=messages)
        elapsed = perf_counter() - started
        after = end_offsets(docker, topic)
        if before.keys() != after.keys() or any(after[part] < before[part] for part in before):
            raise ValueError("Kafka partitions or offsets changed during replay")
        appended = sum(after[part] - before[part] for part in before)
        if appended != count:
            raise ValueError(f"Kafka appended {appended} records, expected {count}")
    return {
        "replay_id": replay_id, "messages": count, "producer_seconds": round(elapsed, 3),
        "messages_per_second_including_producer_startup": round(count / elapsed, 1),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--replay-id", required=True)
    args = parser.parse_args()
    try:
        if (not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.topic)
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.replay_id)):
            raise ValueError("Use a short topic and replay ID composed of letters, digits, hyphens and underscores")
        print(json.dumps(publish(args.source, args.manifest, args.topic, args.replay_id), indent=2))
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"error: {error}\n")


if __name__ == "__main__":
    main()
