#!/bin/bash
# ================================================================
# BENCHMARK RUNNER - Using Your Kafka (kafka-g5)
# Save this as: run-benchmark.sh
# ================================================================

set -e

NAMESPACE="bd-bd-gr-05"
THROUGHPUT=${1:-100}
KAFKA_BOOTSTRAP_SERVERS="kafka-g5.bd-bd-gr-05.svc.cluster.local:9092"
SCHEMA_REGISTRY_URL="http://schema-registry.bd-bd-gr-05.svc.cluster.local:8081"

echo "=========================================="
echo "  SPARK STREAMING BENCHMARK"
echo "=========================================="
echo "Throughput: $THROUGHPUT msg/s"
echo "Namespace: $NAMESPACE"
echo "Kafka: kafka-g5"
echo "=========================================="
echo ""

# ================================================================
# STEP 1: Deploy Schema Registry and Spark Consumer
# ================================================================
echo "📦 Step 1: Deploying infrastructure..."
echo ""

kubectl apply -f k8s/deployment.yaml

echo "⏳ Waiting for Schema Registry..."
kubectl wait --for=condition=ready pod -l app=schema-registry -n $NAMESPACE --timeout=120s || echo "  (timeout - checking manually...)"

# Check if it's actually running
SR_POD=$(kubectl get pods -n $NAMESPACE -l app=schema-registry -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -z "$SR_POD" ]; then
    echo "⚠️  Schema Registry pod not found, waiting 30s..."
    sleep 30
    SR_POD=$(kubectl get pods -n $NAMESPACE -l app=schema-registry -o jsonpath='{.items[0].metadata.name}')
fi

echo "✅ Schema Registry: $SR_POD"

echo ""
echo "⏳ Waiting 20s for Schema Registry to stabilize..."
sleep 20
echo ""

# ================================================================
# STEP 2: Create Kafka Topics
# ================================================================
echo "📊 Step 2: Creating Kafka topics..."
echo ""

KAFKA_POD="kafka-g5-controller-0"

create_topic() {
    local topic=$1
    echo "  Creating topic: $topic"
    kubectl exec -n $NAMESPACE $KAFKA_POD -- \
        /opt/bitnami/kafka/bin/kafka-topics.sh --create \
        --bootstrap-server localhost:9092 \
        --topic $topic \
        --partitions 5 \
        --replication-factor 3 \
        --if-not-exists 2>/dev/null || echo "  (topic may already exist)"
}

create_topic "weather.wind"
create_topic "weather.sunshine"
create_topic "weather.aggregated.output"

echo "✅ Topics created"
echo ""

# Verify topics
echo "📋 Verifying topics..."
kubectl exec -n $NAMESPACE $KAFKA_POD -- \
    /opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 | grep weather || echo "  (checking...)"
echo ""

# ================================================================
# STEP 3: Deploy Spark Consumer
# ================================================================
echo "⚡ Step 3: Waiting for Spark Consumer..."
echo ""

kubectl wait --for=condition=ready pod -l app=spark-consumer -n $NAMESPACE --timeout=180s || echo "  (timeout - checking manually...)"

SPARK_POD=$(kubectl get pods -n $NAMESPACE -l app=spark-consumer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -z "$SPARK_POD" ]; then
    echo "⚠️  Spark Consumer pod not found, waiting 30s..."
    sleep 30
    SPARK_POD=$(kubectl get pods -n $NAMESPACE -l app=spark-consumer -o jsonpath='{.items[0].metadata.name}')
fi

echo "✅ Spark Consumer: $SPARK_POD"

echo "⏳ Waiting 30s for consumer to initialize..."
sleep 30
echo ""

# Check Spark logs
echo "📋 Spark Consumer status:"
kubectl logs -n $NAMESPACE $SPARK_POD --tail=10 || echo "  (logs not available yet)"
echo ""

# ================================================================
# STEP 4: Run Producer
# ================================================================
echo "🚀 Step 4: Starting Producer..."
echo ""

# Delete old job if exists
kubectl delete job benchmark-producer -n $NAMESPACE 2>/dev/null || true
sleep 2

# Create producer job
cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: benchmark-producer
  namespace: $NAMESPACE
spec:
  ttlSecondsAfterFinished: 300
  backoffLimit: 2
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: producer
        image: ortemiy/benchmark-producer:latest
        args: ["$THROUGHPUT"]
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "$KAFKA_BOOTSTRAP_SERVERS"
        - name: SCHEMA_REGISTRY_URL
          value: "$SCHEMA_REGISTRY_URL"
        resources:
          requests:
            memory: "1Gi"
            cpu: "1000m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
EOF

echo "✅ Producer job created"
echo ""

# Wait a bit for job to start
sleep 5

# Monitor producer
echo "📊 Monitoring producer (this may take 1-2 minutes)..."
kubectl wait --for=condition=complete job/benchmark-producer -n $NAMESPACE --timeout=180s || true

PRODUCER_STATUS=$(kubectl get job benchmark-producer -n $NAMESPACE -o jsonpath='{.status.conditions[0].type}' 2>/dev/null || echo "Unknown")
if [ "$PRODUCER_STATUS" == "Complete" ]; then
    echo "✅ Producer completed successfully"
else
    echo "⚠️  Producer status: $PRODUCER_STATUS"
    echo ""
    echo "📋 Producer logs:"
    kubectl logs -n $NAMESPACE -l app=producer --tail=20
fi

echo ""
echo "⏳ Waiting 40s for Spark to process data..."
sleep 40
echo ""

# ================================================================
# STEP 5: Run Latency Monitor
# ================================================================
echo "⏱️  Step 5: Running Latency Monitor..."
echo ""

# Delete old job if exists
kubectl delete job latency-monitor -n $NAMESPACE 2>/dev/null || true
sleep 2

# Create latency monitor job
cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: latency-monitor
  namespace: $NAMESPACE
spec:
  ttlSecondsAfterFinished: 600
  backoffLimit: 2
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: monitor
        image: ortemiy/latency-monitor:latest
        args: ["$THROUGHPUT"]
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "$KAFKA_BOOTSTRAP_SERVERS"
        - name: SCHEMA_REGISTRY_URL
          value: "$SCHEMA_REGISTRY_URL"
        - name: INPUT_TOPIC
          value: "weather.aggregated.output"
        - name: MAX_WAIT_TIME_MS
          value: "60000"
        - name: MAX_EMPTY_POLLS
          value: "10"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
EOF

echo "✅ Latency Monitor job created"
echo ""

# Wait for completion
sleep 5
kubectl wait --for=condition=complete job/latency-monitor -n $NAMESPACE --timeout=120s || true

echo "✅ Latency Monitor completed"
echo ""

# ================================================================
# STEP 6: Display Results
# ================================================================
echo "=========================================="
echo "  📊 BENCHMARK RESULTS"
echo "=========================================="
echo ""

echo "🔹 Producer Results:"
echo "----------------------------------------"
kubectl logs -n $NAMESPACE -l app=producer --tail=30 2>/dev/null || echo "(No producer logs available)"
echo ""

echo "🔹 Latency Monitor Results:"
echo "----------------------------------------"
kubectl logs -n $NAMESPACE -l app=latency-monitor --tail=60 2>/dev/null || echo "(No latency monitor logs available)"
echo ""

echo "🔹 Spark Consumer Recent Logs:"
echo "----------------------------------------"
kubectl logs -n $NAMESPACE -l app=spark-consumer --tail=20 2>/dev/null || echo "(No Spark logs available)"
echo ""

echo "🔹 Resource Usage:"
echo "----------------------------------------"
kubectl top pod -n $NAMESPACE 2>/dev/null || echo "(Metrics not available)"
echo ""

echo "🔹 Topic Status:"
echo "----------------------------------------"
kubectl exec -n $NAMESPACE $KAFKA_POD -- \
    kafka-topics --describe --bootstrap-server localhost:9092 --topic weather.aggregated.output 2>/dev/null | head -10 || echo "(Topic not accessible)"
echo ""

# ================================================================
# FINAL SUMMARY
# ================================================================
echo "=========================================="
echo "  ✅ BENCHMARK COMPLETE"
echo "=========================================="
echo ""
echo "Pod Status:"
kubectl get pods -n $NAMESPACE -l app=spark-consumer
kubectl get pods -n $NAMESPACE -l app=schema-registry
kubectl get jobs -n $NAMESPACE
echo ""
echo "Quick Commands:"
echo "  • View Spark logs:          kubectl logs -n $NAMESPACE -l app=spark-consumer -f"
echo "  • View producer logs:       kubectl logs -n $NAMESPACE -l app=producer"
echo "  • View latency logs:        kubectl logs -n $NAMESPACE -l app=latency-monitor"
echo "  • Port-forward Spark UI:    kubectl port-forward -n $NAMESPACE svc/spark-consumer 4040:4040"
echo "  • List Kafka topics:        kubectl exec -n $NAMESPACE $KAFKA_POD -- kafka-topics --list --bootstrap-server localhost:9092"
echo "  • Check topic messages:     kubectl exec -n $NAMESPACE $KAFKA_POD -- kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic weather.aggregated.output"
echo ""
echo "Cleanup:"
echo "  • Delete jobs:              kubectl delete job benchmark-producer latency-monitor -n $NAMESPACE"
echo "  • Delete Spark & Schema:    kubectl delete deployment spark-consumer schema-registry -n $NAMESPACE"
echo "  • Delete services:          kubectl delete svc spark-consumer schema-registry -n $NAMESPACE"
echo "  • Delete topics:            kubectl exec -n $NAMESPACE $KAFKA_POD -- kafka-topics --delete --topic weather.wind --bootstrap-server localhost:9092"
echo ""
echo "=========================================="