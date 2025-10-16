# Multi-stage build
FROM hseeberger/scala-sbt:eclipse-temurin-11.0.17_8_1.8.2_3.2.2 AS builder

WORKDIR /app

# Copy project files
COPY build.sbt .
COPY project ./project
COPY src ./src

# Build fat JAR
RUN sbt clean assembly

# Runtime stage
FROM eclipse-temurin:11-jre-jammy

WORKDIR /app

# Copy JAR from builder
COPY --from=builder /app/target/scala-3.*/latency-monitor-assembly-*.jar /app/latency-monitor.jar

# Create output directory
RUN mkdir -p /app/benchmark-results

# Environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV SCHEMA_REGISTRY_URL=http://localhost:8081
ENV INPUT_TOPIC=weather.aggregated.output
ENV OUTPUT_DIR=/app/benchmark-results
ENV MAX_WAIT_TIME_MS=30000
ENV MAX_EMPTY_POLLS=5

# Entry point
ENTRYPOINT ["java", "-Xmx512m", "-Xms256m", "-jar", "/app/latency-monitor.jar"]

# Default argument (throughput for labeling)
CMD ["100"]