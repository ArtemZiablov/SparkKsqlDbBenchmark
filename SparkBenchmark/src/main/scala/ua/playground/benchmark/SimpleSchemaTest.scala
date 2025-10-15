package ua.playground.benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.avro.functions.*

/**
 * Simple test to verify schema is correct
 */
object SimpleSchemaTest:

  def main(args: Array[String]): Unit =
    val spark = SparkSession
      .builder()
      .appName("SchemaTest")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits.*

    println("\n" + "=" * 60)
    println("SCHEMA VERIFICATION TEST")
    println("=" * 60)

    val avroSchema = """
    {
      "type": "record",
      "name": "WeatherData",
      "namespace": "ua.playground.benchmark",
      "fields": [
        {"name": "timeObserved", "type": "string"},
        {"name": "stationId", "type": "int"},
        {"name": "stationName", "type": "string"},
        {"name": "metric", "type": "string"},
        {"name": "value", "type": "double"},
        {"name": "producer_ts", "type": "long"}
      ]
    }
    """

    // Read from Kafka
    val kafkaDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "weather.wind")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    println("\n1. Raw Kafka DataFrame schema:")
    kafkaDF.printSchema()

    // ✅ Strip Schema Registry prefix (5 bytes) before decoding
    val strippedDF = kafkaDF
      .withColumn("avro_value", expr("substring(value, 6, length(value) - 5)"))

    // Decode Avro
    val decodedDF = strippedDF
      .select(
        from_avro($"avro_value", avroSchema).as("data"),
        $"timestamp",
        $"partition",
        $"offset"
      )

    println("\n2. After Avro decoding:")
    decodedDF.printSchema()

    // Extract nested fields
    val extractedDF = decodedDF
      .select(
        $"data.timeObserved",
        $"data.stationId",
        $"data.stationName",
        $"data.metric",
        $"data.value",
        $"data.producer_ts",
        $"timestamp".as("kafka_timestamp")
      )

    println("\n3. After extracting nested fields:")
    extractedDF.printSchema()

    // Show sample data
    println("\n4. Sample data (first 5 rows):")
    extractedDF.show(5, truncate = false)

    // Check producer_ts values
    println("\n5. Producer timestamp statistics:")
    extractedDF.select(
      min("producer_ts").as("min_producer_ts"),
      max("producer_ts").as("max_producer_ts"),
      count("producer_ts").as("count")
    ).show()

    println("\n6. Sample producer_ts values:")
    extractedDF.select("stationId", "producer_ts").show(10, truncate = false)

    println("\n" + "=" * 60)
    println("TEST COMPLETE")
    println("=" * 60)

    spark.stop()