"""Exercise revision and quarantine handling in a real Spark process."""
from datetime import datetime, timezone
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id, lit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from stream_job import bronze_records, hourly_rollup


def envelope(event_key, record_key, value, revision):
    return json.dumps({"replay_id": "fixture", "event": {
        "station": "fixture", "series_id": "fixture", "parameter": "00060",
        "event_time": "2024-01-08T00:30:00.000000Z", "event_key": event_key,
        "record_key": record_key, "value": value, "unit": "ft^3/s",
        "source_revision_time": revision,
    }})


def main():
    spark = (
        SparkSession.builder.appName("River flow revision fixture")
        .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        old = "2024-01-08T01:00:00.000000Z"
        new = "2024-01-09T01:00:00.000000Z"
        keys = [char * 64 for char in "abcdef012"]
        broker_time = datetime(2026, 11, 18, tzinfo=timezone.utc)
        schema = StructType([
            StructField("key", StringType()), StructField("value", StringType()),
            StructField("timestamp", TimestampType()),
        ])
        data = [
            (keys[0], envelope(keys[0], keys[1], 19.5, new), broker_time),
            (keys[0], envelope(keys[0], keys[2], 17.0, old), broker_time),
            (keys[0], envelope(keys[0], keys[7], 14.0, old), broker_time),
            (keys[0], envelope(keys[0], keys[1], 19.5, new), broker_time),
            (keys[3], envelope(keys[3], keys[4], 10.0, new), broker_time),
            (keys[3], envelope(keys[3], keys[5], 30.0, new), broker_time),
            (keys[6], "broken JSON", broker_time),
            (keys[6], envelope(keys[6], keys[8], "invalid flow", old), broker_time),
        ]
        source = (
            spark.createDataFrame(data, schema).withColumn("topic", lit("fixture"))
            .withColumn("partition", lit(0)).withColumn("offset", monotonically_increasing_id())
        )
        decoded = bronze_records(source)
        assert decoded.where("not is_valid").count() == 2
        revisions, ties, latest, hourly = hourly_rollup(decoded.where("is_valid"))
        assert revisions.count() == 5
        assert ties.count() == 1
        assert latest.count() == 1
        assert float(hourly.first().mean_flow) == 19.5
        print("Spark fixture confirmed data quarantine, delayed old revisions, duplicates and revision conflicts.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
