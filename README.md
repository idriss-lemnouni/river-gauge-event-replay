# River Gauge Event-Time Replay

Implement a bounded historical input contract for streamflow observations from USGS. The acquisition stage writes JSON Lines plus an archive manifest, verifies pagination and interval boundaries, and normalizes time offsets to UTC.

The verified acquisition returned 673 discharge observations in two pages for USGS-01646500 between 8 and 15 January 2024 inclusive. Discharge is reported in ft³/s. The local archive checksum and request parameters are recorded in the [acquisition manifest](reports/potomac-acquisition.json).

```sh
python fetch_observations.py --station USGS-01646500 --start 2024-01-08T00:00:00Z --end 2024-01-15T00:00:00Z --out data/raw/potomac-2024-01
python -m unittest discover -s tests -v
```

Python 3.11+ and standard library. The observation key identifies the series and instant. The record key includes provider revision metadata and the value. Retried pages can repeat observations without losing changed versions. A source revision timestamp describes the returned historical record. Actual arrival latency and out-of-order playback will be measured separately after Kafka/Spark integration.

The next milestone is a Kafka producer and a Spark Structured Streaming reader with checkpointed Parquet writes, followed by idempotent revision aggregation and Cassandra/Redis integration. [USGS OGC API](https://api.waterdata.usgs.gov/docs/ogcapi) · [USGS data licensing](https://www.usgs.gov/data-management/data-licensing)
