# Spark Structured Streaming Benchmark

This guide explains how to run the Spark streaming benchmark for the weather data aggregation pipeline. The benchmark measures end-to-end latency for processing weather streams through Kafka and Spark with 1-minute tumbling windows.

## Prerequisites

- Java 11
- Docker (for Kafka, Schema Registry, Zookeeper)
- Scala 3.3.x
- SBT (Scala Build Tool)
- macOS or Linux

## Architecture Overview

The benchmark consists of three main components:

1. **Producer** (`producer/`): Sends weather data to Kafka topics at a target throughput
2. **Consumer** (`spark-consumer/`): Apache Spark application that aggregates data in tumbling windows
3. **Latency Monitor** (`latency-monitor/`): Measures end-to-end latency from producer to aggregated output

Data flow: Producer → Kafka Topics → Spark Consumer → Aggregated Output Topic → Latency Monitor

## Step 1: Build All Components

Build each module to create fat JARs with all dependencies included.

### 1a. Build Producer

```bash
cd producer
sbt clean compile assembly
cd ..
```

**What this does:** Compiles the producer module and creates `target/scala-3.3.7/benchmark-producer.jar`

### 1b. Build Spark Consumer

```bash
cd spark-consumer
sbt clean compile assembly
cd ..
```

**What this does:** Compiles the Spark consumer and creates `target/scala-3.3.7/spark-consumer.jar`

### 1c. Build Latency Monitor

```bash
cd latency-monitor
sbt clean compile assembly
cd ..
```

**What this does:** Compiles the monitoring tool and creates `target/scala-3.3.1/latency-monitor.jar`

## Step 2: Configure Java Home

Set your Java 11 home directory (required for proper compilation and runtime):

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```

**What this does:** Points all Java operations to Java 11. Add this to your shell profile (`.zshrc`, `.bash_profile`) to make it permanent.

## Step 3: Reset Benchmark Environment

Clean up any previous benchmark runs and prepare fresh Kafka topics:

```bash
cd producer
chmod +x *.sh
./reset-benchmark.sh
cd ..
```

**What this does:**
- Stops any running Java processes from previous benchmarks
- Deletes old Kafka topics and Spark checkpoints
- Creates fresh `weather.wind`, `weather.sunshine`, and `weather.aggregated.output` topics
- Clears old benchmark result files

**Output you'll see:**
```
✅ All processes stopped
✅ Topics deleted
✅ Checkpoints cleaned
✅ Topics created: weather.wind, weather.sunshine, weather.aggregated.output
```

## Step 4: Start Spark Consumer

Launch the Spark structured streaming consumer that will aggregate incoming data:

```bash
cd spark-consumer
java -jar target/scala-3.3.7/spark-consumer.jar 100
cd ..
```

**What this does:**
- Starts a Spark application running in local mode (`local[*]` uses all available cores)
- Connects to Kafka and subscribes to `weather.wind` and `weather.sunshine` topics
- Sets up 1-minute tumbling windows for aggregation
- Begins listening for incoming messages (does nothing yet - waiting for data)
- Outputs aggregated results to `weather.aggregated.output` topic

**Parameter:** `100` = target throughput in msg/s per topic (used for configuration, not actual rate limit)

**Output you'll see:**
```
✅ Spark Session created (OPTIMIZED)
✅ Connected to Kafka topics: weather.wind, weather.sunshine
✅ Windowed aggregation created (1 minute windows)
🚀 STARTING STREAMING QUERY TO KAFKA (OPTIMIZED)
✅ Aggregated results streaming to: weather.aggregated.output
```

**Keep this terminal running.** The Spark consumer must stay active throughout the benchmark.

## Step 5: Run Data Producer

In a new terminal, run the producer to send test data through the pipeline:

```bash
cd producer
java -jar target/scala-3.3.7/benchmark-producer.jar 100
cd ..
```

**What this does:**
- Reads weather data from CSV files (`data/wind_test.csv`, `data/sunshine_test.csv`)
- Sends 1,000 messages to each topic (2,000 total)
- Targets 100 msg/s throughput per topic
- Uses Avro serialization with Schema Registry
- Records producer timestamp for each message (used for latency calculation)
- Sends wind data and sunshine data concurrently

**Parameter:** `100` = target throughput in messages/second per topic

**Output you'll see:**
```
📖 Reading data from: data/wind_test.csv
📊 Total messages to send: 1000
⏱️  Delay between messages: 10ms
🎯 Target throughput: 100 msg/s
📤 Sent: 1000 msgs | ✅ Success: 1000 | ❌ Errors: 0
```

**Duration:** ~70 seconds for producer to complete

**Go back to Spark consumer terminal** and watch for output messages showing aggregation progress.

## Step 6: Measure Latency

After producer finishes and Spark has finished processing (wait ~60 seconds), run the latency monitor in a new terminal:

```bash
cd latency-monitor
java -jar target/scala-3.3.1/latency-monitor.jar 100
cd ..
```

**What this does:**
- Connects to Kafka and reads from `weather.aggregated.output` topic
- Deserializes Avro messages from Spark
- Extracts the producer timestamp (when message was created) and processing end timestamp (when aggregation completed)
- Calculates end-to-end latency for each message
- Computes statistics (average, median, P95, P99, etc.)
- Generates a report with results

**Parameter:** `100` = target throughput (for reporting/filtering purposes)

**Duration:** ~10-30 seconds (waits up to 60 seconds for data)

## Understanding the Latency Monitor Output

### Header Information

```
Target Throughput: 100 msg/s
Input Topic: weather.aggregated.output
Kafka Brokers: localhost:9092
```

Shows the benchmark configuration being measured.

### Format Detection

```
🔍 Detected: Direct Avro format
```

Confirms the data is using native Avro serialization (optimal, no wire format overhead).

### Latency Statistics Explained

```
Average (Mean):      5595.57 ms
Median (P50):        5996.00 ms
P95:                11996.00 ms
P99:                11996.00 ms
Min:                 1353.00 ms
Max:                11996.00 ms
Std Deviation:       3390.75 ms
```

| Metric | Meaning | Expected Range |
|--------|---------|-----------------|
| **Average** | Mean latency across all windows | 5-8 seconds (depends on window size) |
| **Median (P50)** | 50th percentile - middle value | Similar to average for uniform distribution |
| **P95** | 95% of messages processed faster than this | ~2x average (upper tail) |
| **P99** | 99% of messages processed faster than this | Worst-case latency for 99% |
| **Min** | Fastest window processed | Early/special case |
| **Max** | Slowest window processed | Last window in batch |
| **Std Dev** | Variance in latencies | Lower = more consistent |

### Example Interpretation

With your results:
- **Average 5.6 seconds:** Most aggregations complete within ~5-6 seconds of first message
- **P99 12 seconds:** Worst case, 99% of aggregations complete by 12 seconds
- **Std Dev 3.4 seconds:** Moderate variance (typical for streaming systems)

### Analysis Output

```
✅ GOOD - Acceptable latency
✅ GOOD - P99 acceptable
✅ EXCELLENT - Very consistent
```

The monitor automatically evaluates your latency:
- **Average < 5s:** Excellent | 5-30s: Good | 30-60s: Moderate | > 60s: High
- **P99 < 10s:** Excellent | 10-60s: Good | > 60s: High
- **Std Dev < 5s:** Excellent | 5-15s: Good | > 15s: Moderate

### Top Latency Windows

```
1. sunshine @ Silstrup: 11996.00 ms
   Window: 2025-10-20 23:13:00 -> 2025-10-20 23:14:00
```

Shows which metrics/stations had the highest latencies. Use this to identify bottlenecks:
- If certain stations always slow: Check data quality or kafka partition balance
- If certain metrics slow: Certain aggregations might be more expensive
- If all similar: System is balanced

### Sample Data

```
Sample Count: 14 windows
Total records processed: 14
Valid latency data points: 14
Duration: 10.519 seconds
```

Indicates:
- **14 windows:** ~14 minutes of 1-minute windows were processed and reported
- **All valid:** No deserialization or data corruption errors
- **Collection took 10.5 seconds:** Monitor read all 14 records within 10 seconds

### Report File

```
📄 Report saved to: ./benchmark-results/latency-report-100msg-s-1760994848977.txt
```

A text report is saved with full details for archiving/comparison.

## Typical Results Summary

For 100 msg/s throughput with 1-minute windows:
- **Average latency:** 5-7 seconds (normal - waiting for window to close)
- **P99 latency:** 11-14 seconds (acceptable for aggregation workload)
- **Consistency:** Std Dev 3-4 seconds (good predictability)
- **Success rate:** All 14 windows valid (100%)

This indicates a **production-ready** streaming setup for weather aggregation.

## Troubleshooting

### "Topic not found" error
```bash
# Run reset to create topics
./reset-benchmark.sh
```

### Spark consumer hangs
```bash
# Kill the Spark process and restart
pkill -f spark-consumer.jar
java -jar spark-consumer/target/scala-3.3.7/spark-consumer.jar 100
```

### "No latency data found"
- Wait longer for Spark to finish processing (~2-3 minutes after producer completes)
- Check that producer actually sent data: `docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic weather.wind --from-beginning --max-messages 1`

### Build failures
```bash
# Clean build caches
cd <module>
rm -rf target .bsp
sbt clean compile assembly
```

## Performance Tuning

To improve latency, modify these in `spark-consumer/src/main/scala/...`:

1. **Smaller windows:** Change `WINDOW_DURATION` from "1 minute" to "30 seconds"
2. **Faster triggers:** Change `TRIGGER_INTERVAL` from "2 seconds" to "1 second"
3. **More parallelism:** Change `SHUFFLE_PARTITIONS` from "10" to "20"

Each change affects latency vs throughput trade-offs.

## Notes

- The benchmark uses 100 msg/s as the target, but actual throughput may be 80-90 msg/s due to producer timing precision
- Latencies include network, serialization, windowing, and Kafka output delays
- Running on Apple Silicon (ARM64) may show higher latencies due to Docker emulation overhead
- Results are sensitive to CPU load and network latency on your machine