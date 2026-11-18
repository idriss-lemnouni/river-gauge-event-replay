"""Compare the two checkpointed passes of the reproducible Kafka/Spark replay."""
import argparse
import json
from pathlib import Path


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def verify(run: Path, manifest: Path) -> dict:
    expected = sum(station["records"] for station in read(manifest)["stations"].values())
    first = read(run / "first" / "metrics.json")
    second = read(run / "second" / "metrics.json")
    before = read(run / "progress-first.json")
    after = read(run / "progress-second.json")
    producers = [read(run / f"{name}-producer.json") for name in ("first", "second")]
    if before["query_id"] != after["query_id"]:
        raise ValueError("Spark did not resume the original query ID")
    if not before["batches"] or not after["batches"]:
        raise ValueError("Both streaming passes must complete at least one batch")
    if after["batches"][0]["batch_id"] <= before["batches"][-1]["batch_id"]:
        raise ValueError("Spark did not continue the first checkpointed batch sequence")
    if any(entry["messages"] != expected for entry in producers):
        raise ValueError("The producers did not each send the full snapshot")
    if before["input_rows"] != expected or after["input_rows"] != expected:
        raise ValueError("Spark did not consume only the new records in each pass")
    if first["bronze_rows"] != expected or second["bronze_rows"] != expected * 2:
        raise ValueError("The bronze history should retain both publishes")
    if (first["replayed_duplicates"] != 0 or second["replayed_duplicates"] != expected
            or first["latest_observations"] != expected or second["latest_observations"] != expected
            or first["invalid_rows"] or second["invalid_rows"]):
        raise ValueError("Replayed duplicates changed the valid analytic population")
    if not first["hourly_groups"] or first["hourly_sha256"] != second["hourly_sha256"]:
        raise ValueError("The hourly rollup changed after a duplicate replay")
    return {
        "source_observations": expected,
        "first": first, "second": second,
        "stream": {"first": before, "second": after},
        "producers": producers,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.run, args.manifest)
    (args.run / "verification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
