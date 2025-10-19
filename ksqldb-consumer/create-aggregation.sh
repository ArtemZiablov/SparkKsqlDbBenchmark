#!/bin/bash

echo "Creating aggregation table..."

docker exec ksqldb-cli ksql http://ksqldb-server:8088 << SQL
SET 'auto.offset.reset' = 'earliest';

CREATE TABLE weather_aggregated_output WITH (
  KAFKA_TOPIC='weather.aggregated.output',
  PARTITIONS=1,
  VALUE_FORMAT='JSON'
) AS
SELECT
  stationId,
  LATEST_BY_OFFSET(stationName) AS stationName,
  TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_end,
  metric,
  AVG(value) AS avg_value,
  MIN(value) AS min_value,
  MAX(value) AS max_value,
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

SHOW TABLES;
SHOW QUERIES;
SQL
