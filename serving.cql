CREATE KEYSPACE IF NOT EXISTS river_flow
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS river_flow.hourly_by_series_day (
    snapshot text,
    station text,
    series_id text,
    unit text,
    day date,
    hour_utc timestamp,
    observations int,
    reported_values int,
    mean_flow decimal,
    min_flow decimal,
    max_flow decimal,
    PRIMARY KEY ((snapshot, station, series_id, unit, day), hour_utc)
) WITH CLUSTERING ORDER BY (hour_utc ASC);
