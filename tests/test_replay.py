"""Snapshot validation before delivery to Kafka."""
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from replay import prepare_replay
from fetch_observations import canonical


class ReplayTests(unittest.TestCase):
    def test_prepare_keeps_kafka_key_and_observation_identity(self):
        event = {"event_key": "a" * 64, "record_key": "b" * 64, "value": 7}
        data = canonical(event) + b"\n"
        with TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "observations.jsonl"
            manifest = root / "manifest.json"
            source.write_bytes(data)
            manifest.write_text(json.dumps({
                "sha256": hashlib.sha256(data).hexdigest(),
                "stations": {"fixture": {"records": 1}},
            }), encoding="utf-8")

            with prepare_replay(source, manifest, "second-replay") as (stream, count):
                key, payload = stream.readline().split(b"\t", 1)
                self.assertEqual(count, 1)
                self.assertEqual(key.decode(), event["event_key"])
                self.assertEqual(json.loads(payload), {"replay_id": "second-replay", "event": event})

            source.write_bytes(data.replace(b"7", b"8"))
            with self.assertRaisesRegex(ValueError, "differs from its manifest"):
                with prepare_replay(source, manifest, "changed"):
                    self.fail("An altered snapshot must be rejected before publishing")


if __name__ == "__main__":
    unittest.main()
