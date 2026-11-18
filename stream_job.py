"""Checkpoint Kafka offsets and calculate revision-aware hourly USGS flow."""
import argparse
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession, Window, functions as f

ENVELOPE = """
replay_id STRING,
event STRUCT<series_id:STRING, station:STRING, parameter:STRING, event_time:TIMESTAMP,
event_key:STRING, source_revision_time:TIMESTAMP, value:DOUBLE, unit:STRING,
approval:STRING, statistic_id:STRING, record_key:STRING>,
_corrupt_record STRING
"""


def bronze_records(source):
    decoded = source.select(
        f.col("key").cast("string").alias("kafka_key"),
        f.col("value").cast("string").alias("payload"),
        "topic", "partition", "offset",
        f.col("timestamp").alias("broker_time"),
    ).withColumn("decoded", f.from_json("payload", ENVELOPE, {
        "columnNameOfCorruptRecord": "_corrupt_record",
    }))
    parsed = decoded.select(
        "kafka_key", "payload", "topic", "partition", "offset", "broker_time",
        "decoded.replay_id", "decoded._corrupt_record", "decoded.event.*",
    )
    valid = (
        f.col("_corrupt_record").isNull()
        & f.col("event_key").rlike("^[0-9a-f]{64}$")
        & f.col("record_key").rlike("^[0-9a-f]{64}$")
        & (f.col("event_key") == f.col("kafka_key"))
        & (f.col("parameter") == "00060")
        & (f.length("series_id") > 0)
        & (f.length("station") > 0)
        & (f.length("unit") > 0)
        & (f.length("replay_id") > 0)
        & f.col("event_time").isNotNull()
        & (f.col("value").isNull() | (~f.isnan("value") & (f.abs("value") < float("inf"))))
    )
    return parsed.withColumn("is_valid", f.coalesce(valid, f.lit(False))).withColumn(
        "written_at", f.current_timestamp()
    )


def ingest(spark, args):
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )
    query = (
        bronze_records(kafka).writeStream.format("parquet").outputMode("append")
        .option("path", str(args.bronze))
        .option("checkpointLocation", str(args.checkpoint))
        .trigger(availableNow=True).start()
    )
    query.awaitTermination()
    progress = [
        {"batch_id": entry["batchId"], "input_rows": entry["numInputRows"],
         "trigger_ms": entry.get("durationMs", {}).get("triggerExecution")}
        for entry in query.recentProgress
    ]
    metrics = {
        "query_id": str(query.id),
        "input_rows": sum(batch["input_rows"] for batch in progress),
        "batches": progress,
    }
    args.progress.parent.mkdir(parents=True, exist_ok=True)
    args.progress.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    return metrics


def hourly_rollup(accepted):
    revisions = accepted.dropDuplicates(["record_key"])
    rank = Window.partitionBy("event_key").orderBy(f.col("source_revision_time").desc_nulls_last())
    candidates = revisions.withColumn("revision_rank", f.dense_rank().over(rank)).where("revision_rank = 1")
    ties = (
        candidates.groupBy("event_key").count().where(f.col("count") > 1).select("event_key")
    )
    # A tie between the newest revisions needs adjudication before aggregation.
    latest = candidates.join(ties, on="event_key", how="left_anti")
    discharge = f.col("value").cast("decimal(26,6)")
    hourly = latest.groupBy("station", "series_id", "unit", f.date_trunc("hour", "event_time").alias("hour_utc")).agg(
        f.count("*").alias("observations"),
        f.count(discharge).alias("reported_values"),
        f.avg(discharge).alias("mean_flow"),
        f.min(discharge).alias("min_flow"),
        f.max(discharge).alias("max_flow"),
    )
    return revisions, ties, latest, hourly


def aggregate(spark, args):
    if args.out.exists():
        raise ValueError(f"Choose a new output directory for this snapshot: {args.out}")
    bronze = spark.read.parquet(str(args.bronze))
    accepted = bronze.where("is_valid")
    revisions, ties, latest, hourly = hourly_rollup(accepted)
    # Kafka metadata and replay labels are deliberately excluded from the analytics digest.
    digest = hashlib.sha256()
    hours = 0
    for row in hourly.orderBy("station", "series_id", "unit", "hour_utc").toJSON().toLocalIterator():
        digest.update(row.encode("utf-8") + b"\n")
        hours += 1
    raw_rows = bronze.count()
    valid_rows = accepted.count()
    unique_records = revisions.count()
    metrics = {
        "bronze_rows": raw_rows, "invalid_rows": raw_rows - valid_rows,
        "valid_rows": valid_rows, "unique_records": unique_records,
        "replayed_duplicates": valid_rows - unique_records, "ambiguous_event_keys": ties.count(),
        "latest_observations": latest.count(), "hourly_groups": hours, "hourly_sha256": digest.hexdigest(),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=args.out.parent) as work:
        stage = Path(work) / "result"
        hourly.write.parquet(str(stage / "hourly"))
        (stage / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
        stage.rename(args.out)
    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    operations = parser.add_subparsers(dest="operation", required=True)
    reader = operations.add_parser("ingest")
    reader.add_argument("--topic", required=True)
    reader.add_argument("--bootstrap", default="kafka:9092")
    reader.add_argument("--checkpoint", type=Path, required=True)
    reader.add_argument("--progress", type=Path, required=True)
    rollup = operations.add_parser("aggregate")
    rollup.add_argument("--out", type=Path, required=True)
    for operation in (reader, rollup):
        operation.add_argument("--bronze", type=Path, required=True)
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("USGS streamflow replay")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = ingest(spark, args) if args.operation == "ingest" else aggregate(spark, args)
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
