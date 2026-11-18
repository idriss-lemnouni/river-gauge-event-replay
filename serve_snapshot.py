"""Load a verified hourly Parquet export into Cassandra and exercise a Redis read cache."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess
from time import perf_counter
from uuid import uuid4

TABLE = "river_flow.hourly_by_series_day"
KEY_FIELDS = ("station", "series_id", "unit")
VALUES = ("mean_flow", "min_flow", "max_flow")
OUTPUT = ("hour_utc", "observations", "reported_values", *VALUES)
COLUMNS = ("snapshot", *KEY_FIELDS, "day", *OUTPUT)
TTL_SECONDS = 900


def snapshot_rows(path: Path, expected: str):
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("Hourly export does not match the validated Spark digest")
    rows = [json.loads(line, parse_float=Decimal) for line in raw.splitlines()]
    if not rows:
        raise ValueError("A snapshot needs at least one hourly row")
    groups = defaultdict(list)
    for row in rows:
        hour = normalize_hour(row["hour_utc"])
        group = (*(row[key] for key in KEY_FIELDS), hour[:10])
        groups[group].append({**row, "hour_utc": hour})
    return groups


def normalize_hour(value: str) -> str:
    moment = datetime.fromisoformat(value)
    if moment.utcoffset() is None or moment.minute or moment.second or moment.microsecond:
        raise ValueError("Expected an hour-aligned timestamp with explicit offset")
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def cql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def cql_number(value) -> str:
    if value is None:
        return "null"
    number = Decimal(value)
    if not number.is_finite():
        raise ValueError("Invalid numeric flow statistic")
    return str(number)


def insert(snapshot, day, row):
    values = [cql_text(snapshot), *(cql_text(row[k]) for k in KEY_FIELDS),
              cql_text(day), cql_text(row["hour_utc"]),
              cql_number(row["observations"]), cql_number(row["reported_values"]),
              *(cql_number(row.get(k)) for k in VALUES)]
    return f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES ({', '.join(values)});"


def query(snapshot, group):
    terms = " AND ".join(f"{k}={cql_text(v)}" for k, v in zip(
        ("snapshot", *KEY_FIELDS, "day"), (snapshot, *group), strict=True
    ))
    return f"SELECT JSON {', '.join(OUTPUT)} FROM {TABLE} WHERE {terms};"


def cli(service, command, input_text=None):
    call = subprocess.run(
        ["docker", "compose", "exec", "-T", service, *command],
        input=input_text, encoding="utf-8", capture_output=True, timeout=120,
    )
    if call.returncode:
        detail = call.stderr.strip() or call.stdout.strip()
        raise ValueError(f"{service} client exited with status {call.returncode}\n{detail}")
    if call.stderr and service == "cassandra":
        failures = ("SyntaxException", "InvalidRequest", "NoHostAvailable", "Unable to connect", "Error from server")
        if any(failure in call.stderr for failure in failures):
            raise ValueError(call.stderr.strip())
    return call.stdout


def execute_cql(lines):
    return cli("cassandra", ["cqlsh", "--no-color"], "\n".join(lines) + "\n")


def normalized(rows):
    def normalize_metric(value):
        return None if value is None else str(Decimal(value).normalize())

    return sorted((
        {
            "hour_utc": normalize_hour(row["hour_utc"]),
            "observations": int(row["observations"]),
            "reported_values": int(row["reported_values"]),
            **{key: normalize_metric(row.get(key)) for key in VALUES},
        }
        for row in rows
    ), key=lambda row: row["hour_utc"])


def read_partition(snapshot, group):
    result = execute_cql([query(snapshot, group)])
    return normalized([
        json.loads(line.strip(), parse_float=Decimal)
        for line in result.splitlines() if line.lstrip().startswith("{")
    ])


def cached_partition(snapshot, group, cache_namespace):
    group_hash = hashlib.sha256(json.dumps(group).encode("utf-8")).hexdigest()[:24]
    key = f"river:{cache_namespace}:{snapshot}:{group_hash}"
    result = cli("redis", ["redis-cli", "--raw", "GET", key]).strip()
    if result:
        return json.loads(result), "redis"
    rows = read_partition(snapshot, group)
    if not rows:
        raise ValueError("Snapshot partition is missing in Cassandra")
    confirmation = cli("redis", ["redis-cli", "-x", "SETEX", key, str(TTL_SECONDS)], json.dumps(rows)).strip()
    if confirmation != "OK":
        raise ValueError("Redis did not confirm the cache write")
    return rows, "cassandra"


def verify(path: Path, replay_proof: Path):
    proof = json.loads(replay_proof.read_text(encoding="utf-8"))
    snapshot = proof["second"]["hourly_sha256"]
    groups = snapshot_rows(path, snapshot)
    expected = sum(map(len, groups.values()))
    if expected != proof["second"]["hourly_groups"]:
        raise ValueError("Hourly row count does not match the validation report")
    for group, rows in groups.items():
        if len({row["hour_utc"] for row in rows}) != len(rows):
            raise ValueError(f"Duplicate primary keys in the verified input: {group}")
    execute_cql([Path(__file__).with_name("serving.cql").read_text(encoding="utf-8")])
    statements = [insert(snapshot, key[-1], row) for key, rows in groups.items() for row in rows]
    readback_totals = []
    for _ in range(2):
        execute_cql(statements)
        actual = {key: read_partition(snapshot, key) for key in groups}
        if actual != {key: normalized(rows) for key, rows in groups.items()}:
            raise ValueError("Cassandra partition values differ from the verified Spark snapshot")
        readback_totals.append(sum(map(len, actual.values())))

    key = max(groups, key=lambda group: len(groups[group]))
    name = uuid4().hex
    start = perf_counter()
    cold, cold_source = cached_partition(snapshot, key, name)
    cold_ms = (perf_counter() - start) * 1000
    start = perf_counter()
    warm, warm_source = cached_partition(snapshot, key, name)
    warm_ms = (perf_counter() - start) * 1000
    if cold_source != "cassandra" or warm_source != "redis" or cold != warm:
        raise ValueError("Cassandra and Redis returned different results for the same partition")
    return {
        "snapshot_sha256": snapshot, "cassandra_partitions": len(groups),
        "cassandra_rows_per_upsert_pass": readback_totals, "cache_partition_rows": len(warm),
        "cache_ttl_seconds": TTL_SECONDS,
        "cold_read": {"source": cold_source, "wall_ms": round(cold_ms, 3)},
        "warm_read": {"source": warm_source, "wall_ms": round(warm_ms, 3)},
        "timing_context": "Single local Docker CLI end-to-end comparison including process startup.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hourly", required=True, type=Path)
    parser.add_argument("--replay-proof", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    result = verify(args.hourly, args.replay_proof)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
