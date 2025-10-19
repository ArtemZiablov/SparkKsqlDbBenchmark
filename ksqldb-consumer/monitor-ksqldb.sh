#!/bin/bash

# Monitor ksqlDB performance metrics
watch -n 2 'docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "
SHOW QUERIES;
DESCRIBE EXTENDED weather_aggregated_output;
" | grep -E "Query ID|Status|Messages|Throughput|Window"'