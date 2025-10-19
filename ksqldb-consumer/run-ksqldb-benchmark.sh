#!/bin/bash

# Configuration
THROUGHPUT=${1:-100}
PROFILE=${2:-"balanced"}

echo "========================================"
echo "   KSQLDB STREAMING BENCHMARK"
echo "========================================"
echo "Throughput: $THROUGHPUT msg/s"
echo "Profile: $PROFILE"
echo "========================================"
echo ""

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Function to execute ksqlDB statements
execute_ksql() {
    local sql_file=$1
    docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 < "$sql_file"
}

# Function to run ksqlDB query from string
run_ksql_command() {
    local command=$1
    echo "$command" | docker exec -i ksqldb-cli ksql http://ksqldb-server:8088
}

# Start Docker services from project root
echo "🚀 Starting Docker services..."
cd "$PROJECT_ROOT"
docker-compose up -d
cd "$SCRIPT_DIR"
sleep 10

# Clean up existing queries
echo "🧹 Cleaning up existing queries..."
run_ksql_command "TERMINATE ALL;"
sleep 2

# Setup base streams
echo "📊 Setting up base streams..."
execute_ksql "$SCRIPT_DIR/scripts/setup.sql"

# Configure ksqlDB based on profile
case $PROFILE in
  "low-latency")
    echo "🚀 Configuring LOW LATENCY PROFILE"
    SQL_FILE="$SCRIPT_DIR/profiles/low_latency.sql"
    ;;
  "high-throughput")
    echo "📊 Configuring HIGH THROUGHPUT PROFILE"
    SQL_FILE="$SCRIPT_DIR/profiles/high_throughput.sql"
    ;;
  "balanced")
    echo "⚖️ Configuring BALANCED PROFILE"
    SQL_FILE="$SCRIPT_DIR/profiles/balanced.sql"
    ;;
  *)
    echo "❌ Unknown profile: $PROFILE"
    exit 1
    ;;
esac

# Start the aggregation query
echo ""
echo "📊 Starting aggregation query..."
execute_ksql "$SQL_FILE"

# Check the actual jar names in your project
PRODUCER_JAR="$PROJECT_ROOT/producer/target/scala-3.3.1/benchmark-producer.jar"
MONITOR_JAR="$PROJECT_ROOT/latency-monitor/target/scala-3.3.1/latency-monitor.jar"

# Verify jars exist
if [ ! -f "$PRODUCER_JAR" ]; then
    echo "❌ Producer jar not found at: $PRODUCER_JAR"
    echo "Please build the producer project first:"
    echo "  cd $PROJECT_ROOT/producer && sbt assembly"
    exit 1
fi

if [ ! -f "$MONITOR_JAR" ]; then
    echo "❌ Monitor jar not found at: $MONITOR_JAR"
    echo "Please build the latency-monitor project first:"
    echo "  cd $PROJECT_ROOT/latency-monitor && sbt assembly"
    exit 1
fi

# Run producer
echo ""
echo "🚀 Starting data producers..."
java -jar "$PRODUCER_JAR" $THROUGHPUT &
PRODUCER_PID=$!

echo "✅ Producer started (PID: $PRODUCER_PID)"

# Let producer run for some time
echo "⏳ Running producer for 60 seconds..."
sleep 60

# Wait for producer to finish
echo "⏳ Waiting for producer to complete..."
wait $PRODUCER_PID

# Run latency monitor
echo ""
echo "📊 Running latency analysis..."
sleep 5
java -jar "$MONITOR_JAR" $THROUGHPUT

echo ""
echo "✅ Benchmark complete!"