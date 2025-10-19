#!/bin/bash

echo "Testing ksqlDB connection..."

# First, just test if ksqlDB is responsive
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "LIST STREAMS;"

echo "Creating streams..."

# Create streams one by one with error handling
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute \
"CREATE STREAM weather_wind (
  timeObserved VARCHAR,
  stationId INT,
  stationName VARCHAR,
  metric VARCHAR,
  value DOUBLE,
  producer_ts BIGINT
) WITH (
  KAFKA_TOPIC='weather.wind',
  VALUE_FORMAT='JSON'
);" || echo "Stream weather_wind may already exist"

docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute \
"CREATE STREAM weather_sunshine (
  timeObserved VARCHAR,
  stationId INT,
  stationName VARCHAR,
  metric VARCHAR,
  value DOUBLE,
  producer_ts BIGINT
) WITH (
  KAFKA_TOPIC='weather.sunshine',
  VALUE_FORMAT='JSON'
);" || echo "Stream weather_sunshine may already exist"

echo "Checking streams..."
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "SHOW STREAMS;"