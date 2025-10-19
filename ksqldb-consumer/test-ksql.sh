#!/bin/bash

echo "Creating ksqlDB streams and tables..."

docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 << SQL
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM IF NOT EXISTS weather_wind (
  timeObserved VARCHAR,
  stationId INT,
  stationName VARCHAR,
  metric VARCHAR,
  value DOUBLE,
  producer_ts BIGINT
) WITH (
  KAFKA_TOPIC='weather.wind',
  VALUE_FORMAT='AVRO'
);

CREATE STREAM IF NOT EXISTS weather_sunshine (
  timeObserved VARCHAR,
  stationId INT,
  stationName VARCHAR,
  metric VARCHAR,
  value DOUBLE,
  producer_ts BIGINT
) WITH (
  KAFKA_TOPIC='weather.sunshine',
  VALUE_FORMAT='AVRO'
);

CREATE TABLE IF NOT EXISTS weather_aggregated_output WITH (
  KAFKA_TOPIC='weather.aggregated.output',
  PARTITIONS=5,
  VALUE_FORMAT='AVRO'
) AS
SELECT
  stationId,
  LATEST_BY_OFFSET(stationName) AS stationName,
  TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_end,
  metric,
  ROUND(AVG(value), 2) AS avg_value,
  ROUND(MIN(value), 2) AS min_value,
  ROUND(MAX(value), 2) AS max_value,
  COUNT(*) AS message_count,
  MIN(producer_ts) AS min_producer_ts,
  MAX(ROWTIME) AS processing_end_ts
FROM (
  SELECT * FROM weather_wind
  UNION ALL
  SELECT * FROM weather_sunshine
) WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY stationId, metric
EMIT CHANGES;

SHOW QUERIES;
SHOW TABLES;
SQL