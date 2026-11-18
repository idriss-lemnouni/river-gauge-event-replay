from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest
from unittest.mock import patch

from fetch_observations import ENDPOINT, acquire, next_page, normalize, to_utc


def properties(**updates):
    base = {"time_series_id": "series-a", "monitoring_location_id": "USGS-000",
            "parameter_code": "00060", "time": "2024-01-08T01:00:00+01:00",
            "last_modified": "2024-01-10T01:00:00+01:00", "value": "32",
            "unit_of_measure": "ft^3/s", "approval_status": "Approved"}
    return base | updates


class FeedTests(unittest.TestCase):
    def test_offset_and_revisions(self):
        before = normalize(properties(), "USGS-000")
        after = normalize(properties(value="33"), "USGS-000")
        self.assertEqual(before["event_time"], "2024-01-08T00:00:00.000000Z")
        self.assertEqual(before["event_key"], after["event_key"])
        self.assertNotEqual(before["record_key"], after["record_key"])
        with self.assertRaisesRegex(ValueError, "UTC offset"):
            to_utc("2024-01-08T00:00:00")

    def test_disallow_cross_host_pagination(self):
        with self.assertRaisesRegex(ValueError, "pagination endpoint"):
            next_page({"links": [{"rel": "next", "href": "https://example.org/page"}]}, ENDPOINT)

    def test_pagination_preserves_revisions_and_removes_repeat(self):
        original = {"properties": properties()}
        revision = {"properties": properties(value="33")}
        pages = [
            {"type": "FeatureCollection", "features": [original],
             "links": [{"rel": "next", "href": ENDPOINT + "?offset=1"}]},
            {"type": "FeatureCollection", "features": [original, revision], "links": []},
        ]
        with TemporaryDirectory() as folder, patch("fetch_observations.get_page", side_effect=pages):
            root = Path(folder)
            report = acquire(["USGS-000"], "2024-01-08T00:00Z", "2024-01-09T00:00Z", root, 2)
            self.assertEqual(report["stations"]["USGS-000"], {"pages": 2, "records": 2})
            self.assertEqual(report["duplicate_records"], 1)
            lines = (root / "observations.jsonl").read_text().splitlines()
            self.assertEqual([json.loads(line)["value"] for line in lines], [32, 33])


if __name__ == "__main__":
    unittest.main()
