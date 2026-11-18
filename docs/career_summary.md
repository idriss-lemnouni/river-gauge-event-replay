# Career summary

**FR**

- Développement d’un rejeu hydrométrique USGS avec Kafka, Spark Structured Streaming et Parquet.
- Validation de la reprise sur checkpoint avec 673 observations rejouées deux fois et 169 agrégats horaires identiques.

**EN**

- Built a Kafka and Spark Structured Streaming replay for checksummed USGS streamflow observations.
- Verified checkpoint resumption, duplicate delivery handling and stable hourly Parquet rollups across two passes.

## Two-minute explanation

The USGS API returns a versioned observation with its event timestamp. UTC normalization and stable observation and revision keys allow a producer to replay a snapshot without changing its identity. Spark stores the original Kafka messages and offsets, then a separate aggregation selects the latest valid revision of each event. The second batch added 673 deliveries and left all 169 hourly rollups unchanged.

## Five-minute explanation

1. Explain how the source manifest verifies the JSONL snapshot before sending it to Kafka.
2. Show why Kafka messages use the observation ID as their partition key and the record ID for deduplication.
3. Walk through Spark's query ID and batch IDs across a restart. Explain why the raw Parquet history keeps both deliveries.
4. Discuss how invalid payloads and competing newest revisions are handled. Show the synthetic fixture for delayed older revisions.
5. Compare local producer and micro-batch measurements. Explain the limits of a one-station bounded replay and how a serving layer would consume the results.
