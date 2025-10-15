# Multi-stage build for smaller image
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
COPY --from=builder /app/target/scala-3.*/SparkStreamingBenchmark-assembly-*.jar /app/producer.jar

# Copy data files
COPY data ./data

# Create logs directory
RUN mkdir -p /app/logs

# Environment variables (can be overridden)
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV SCHEMA_REGISTRY_URL=http://localhost:8081
ENV THROUGHPUT=100

# Entry point
ENTRYPOINT ["java", "-Xmx1g", "-Xms512m", "-jar", "/app/producer.jar"]

# Default argument (throughput)
CMD ["100"]