"""Acquire and validate a bounded USGS observations replay input."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import Request, urlopen

ENDPOINT = "https://api.waterdata.usgs.gov/ogcapi/v1/collections/continuous/items"
PARAMETER = "00060"


def to_utc(value: str) -> str:
    moment = datetime.fromisoformat(value)
    if moment.utcoffset() is None:
        raise ValueError("Timestamps need an explicit UTC offset")
    return moment.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonical(record) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def normalize(properties: dict, station: str) -> dict:
    series = properties["time_series_id"]
    if (properties["monitoring_location_id"] != station
            or properties["parameter_code"] != PARAMETER or not series):
        raise ValueError("Returned observation does not match the selected station and parameter")
    time = to_utc(properties["time"])
    modified = properties.get("last_modified")
    observed = properties.get("value")
    value = float(observed) if observed not in (None, "") else None
    if value is not None and not math.isfinite(value):
        raise ValueError("A non-finite flow value is invalid")
    key = hashlib.sha256(canonical([series, time])).hexdigest()
    event = {
        "series_id": series, "station": station, "parameter": PARAMETER,
        "event_time": time, "event_key": key,
        "source_revision_time": to_utc(modified) if modified else None,
        "value": value, "unit": properties["unit_of_measure"],
        "approval": properties.get("approval_status"),
        "qualifier": properties.get("qualifier"),
        "statistic_id": properties.get("statistic_id"),
    }
    return event | {"record_key": hashlib.sha256(canonical(event)).hexdigest()}


def next_page(body: dict, current: str) -> str | None:
    for link in body.get("links", []):
        if link["rel"] == "next":
            url = urljoin(current, link["href"])
            expected, observed = urlsplit(ENDPOINT), urlsplit(url)
            if (observed.scheme, observed.netloc, observed.path) != (
                expected.scheme, expected.netloc, expected.path
            ):
                raise ValueError("Unexpected pagination endpoint")
            return url
    return None


def get_page(url: str) -> dict:
    request = Request(url, headers={"Accept": "application/geo+json, application/json"})
    with urlopen(request, timeout=45) as response:
        raw = response.read(8_000_001)
    if len(raw) > 8_000_000:
        raise ValueError("USGS page exceeded 8 MB")
    return json.loads(raw)


def acquire(stations: list[str], start: str, end: str, output: Path, max_pages: int) -> dict:
    start, end = to_utc(start), to_utc(end)
    if start >= end or len(set(stations)) != len(stations):
        raise ValueError("Use an increasing time range and distinct station identifiers")
    output.mkdir(parents=True, exist_ok=True)
    counts = {}
    seen_records = set()
    duplicate_records = 0
    sha = hashlib.sha256()
    with TemporaryDirectory(dir=output) as folder:
        target = Path(folder) / "observations.jsonl"
        with target.open("wb") as destination:
            for station in stations:
                url = ENDPOINT + "?" + urlencode({
                    "f": "json", "monitoring_location_id": station,
                    "parameter_code": PARAMETER, "datetime": f"{start}/{end}", "limit": 500,
                })
                counts[station] = {"pages": 0, "records": 0}
                seen_pages = set()
                while url is not None:
                    if url in seen_pages or len(seen_pages) >= max_pages:
                        raise ValueError(f"Pagination exceeded the bound for {station}")
                    seen_pages.add(url)
                    body = get_page(url)
                    if body.get("type") != "FeatureCollection":
                        raise ValueError("Expected an OGC FeatureCollection")
                    for feature in body["features"]:
                        row = normalize(feature["properties"], station)
                        if not start <= row["event_time"] <= end:
                            raise ValueError("Unexpected event time outside the requested interval")
                        if row["record_key"] in seen_records:
                            duplicate_records += 1
                            continue
                        seen_records.add(row["record_key"])
                        line = canonical(row) + b"\n"
                        sha.update(line)
                        destination.write(line)
                        counts[station]["records"] += 1
                    counts[station]["pages"] += 1
                    url = next_page(body, url)
                if not counts[station]["records"]:
                    raise ValueError(f"No discharge observations returned for {station}")
        manifest = {
            "source": ENDPOINT, "parameter": PARAMETER, "from_utc": start, "to_utc": end,
            "stations": counts, "duplicate_records": duplicate_records, "sha256": sha.hexdigest(),
        }
        report = Path(folder) / "manifest.json"
        report.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        target.replace(output / target.name)
        report.replace(output / report.name)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--station", action="append", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--max-pages", type=int, choices=range(1, 31), default=10, metavar="1–30")
    args = parser.parse_args()
    try:
        result = acquire(args.station, args.start, args.end, args.out, args.max_pages)
        print(json.dumps(result, indent=2))
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"error: {error}\n")


if __name__ == "__main__":
    main()
