import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from serve_snapshot import insert, normalized, query, snapshot_rows


class SnapshotServingTests(unittest.TestCase):
    def test_snapshot_integrity_and_utc_partition(self):
        row = {
            "station": "USGS-test", "series_id": "test", "unit": "ft^3/s",
            "hour_utc": "2024-01-09T01:00:00+02:00", "observations": 4,
            "reported_values": 3, "mean_flow": 110.5, "min_flow": 100.0, "max_flow": 120.0,
        }
        raw = json.dumps(row).encode() + b"\n"
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "hourly.jsonl"
            path.write_bytes(raw)
            groups = snapshot_rows(path, hashlib.sha256(raw).hexdigest())
            key = ("USGS-test", "test", "ft^3/s", "2024-01-08")
            self.assertEqual(list(groups), [key])
            self.assertEqual(normalized(groups[key])[0]["mean_flow"], "110.5")
            self.assertIn("'2024-01-08T23:00:00Z'", insert("sha", key[-1], groups[key][0]))
            with self.assertRaisesRegex(ValueError, "digest"):
                snapshot_rows(path, "0" * 64)

    def test_partition_keys_quote_strings_and_keep_nullable_discharge(self):
        row = {"station": "USGS-'x'", "series_id": "run", "unit": "ft^3/s", "observations": 1,
               "reported_values": 0, "hour_utc": "2024-01-08T23:00:00Z"}
        self.assertIn("station='USGS-''x'''", query("s", ("USGS-'x'", "run", "ft^3/s", "2024-01-08")))
        self.assertTrue(insert("sha", "2024-01-08", row).endswith(", null, null, null);"))


if __name__ == "__main__":
    unittest.main()
