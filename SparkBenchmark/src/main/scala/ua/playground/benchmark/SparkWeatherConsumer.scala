package ua.playground.benchmark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import org.apache.spark.sql.types.*

import java.io.File

object SparkWeatherConsumer:

  def main(args: Array[String]): Unit =
    val throughput = if args.length > 0 then args(0).toInt else 100
    val timestamp = System.currentTimeMillis()

    println("=" * 60)
    println("=== Starting Spark Weather Consumer (Kafka Sink) ===")
    println(s"Throughput: $throughput msg/s")
    println(s"Timestamp: $timestamp")
    println("=" * 60)

    val checkpointLocation = s"/tmp/spark/checkpoints/weather_agg_${throughput}_${timestamp}"
    cleanupCheckpoint(checkpointLocation)

    val spark = SparkSession
      .builder()
      .appName("WeatherStreamingBenchmark")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "5")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.streaming.metricsEnabled", "false")
      .getOrCreate()

    import spark.implicits.*

    spark.sparkContext.setLogLevel("ERROR")
    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    spark.conf.set("spark.sql.streaming.schemaInference", "false")

    println("✅ Spark Session created")
    println(s"📊 Shuffle partitions: 5")
    println(s"🎯 Master: local[*]")
    println(s"💾 Checkpoint location: $checkpointLocation")
    println(s"\n${"=" * 60}")

    val aggregatedAvroSchema = """
    {
      "type": "record",
      "name": "AggregatedWeather",
      "namespace": "ua.playground.benchmark",
      "fields": [
        {"name": "window_start", "type": "string"},
        {"name": "window_end", "type": "string"},
        {"name": "metric", "type": "string"},
        {"name": "stationId", "type": "int"},
        {"name": "stationName", "type": "string"},
        {"name": "avg_value", "type": "double"},
        {"name": "min_value", "type": "double"},
        {"name": "max_value", "type": "double"},
        {"name": "message_count", "type": "long"},
        {"name": "min_producer_ts", "type": "long"},
        {"name": "processing_end_ts", "type": "long"}
      ]
    }
    """

    try
      println("\n📡 Connecting to Kafka topics...")
      val windStream = readKafkaStream(spark, "weather.wind", "wind")
      val sunshineStream = readKafkaStream(spark, "weather.sunshine", "sunshine")
      println("✅ Connected to Kafka topics: weather.wind, weather.sunshine")

      println("\n🔄 Creating windowed aggregation...")
      val aggregatedStream = createWindowedAggregation(
        spark,
        Seq(windStream, sunshineStream)
      )
      println("✅ Windowed aggregation created (10 minute windows)")

      println(s"\n${"=" * 60}")
      println("🚀 STARTING STREAMING QUERY TO KAFKA (AVRO SINK)")
      println(s"${"=" * 60}\n")

      val finalKafkaTopic = "weather.aggregated.output"

      val kafkaOutputDF = aggregatedStream
        .withColumn("key", $"stationId".cast(StringType))
        .withColumn("value", to_avro(
          struct(
            $"window_start".cast(StringType).as("window_start"),
            $"window_end".cast(StringType).as("window_end"),
            $"metric",
            $"stationId",
            $"stationName",
            $"avg_value",
            $"min_value",
            $"max_value",
            $"message_count",
            $"min_producer_ts",
            $"processing_end_ts"
          ),
          aggregatedAvroSchema
        ))
        .select("key", "value")

      val kafkaSinkQuery = kafkaOutputDF.writeStream
        .format("kafka")
        .outputMode("update")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", finalKafkaTopic)
        .option("checkpointLocation", checkpointLocation)
        .queryName("KafkaAggregatedSink")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .start()

      println(s"✅ Aggregated results streaming to: $finalKafkaTopic")
      println(s"✅ Output mode: update (only changed windows)")
      println(s"✅ Checkpoint: $checkpointLocation")

      monitorProgress(kafkaSinkQuery)
      kafkaSinkQuery.awaitTermination()

    catch
      case e: Exception =>
        println(s"\n❌ Error: ${e.getMessage}")
        e.printStackTrace()
    finally
      println("\n🛑 Stopping Spark Session...")
      spark.stop()
      println("✅ Spark Session stopped")

  // ✅ FIXED: Strip Schema Registry prefix before deserializing
  def readKafkaStream(
                       spark: SparkSession,
                       topic: String,
                       streamName: String
                     ): DataFrame =
    import spark.implicits.*

    println(s"  → Reading from topic: $topic")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "1000")
      .option("failOnDataLoss", "false")
      .load()

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

    // ✅ CRITICAL FIX: Remove Schema Registry prefix (5 bytes)
    // KafkaAvroSerializer adds: [magic_byte(1) + schema_id(4)] before Avro data
    val strippedDF = kafkaDF
      .withColumn("avro_value", expr("substring(value, 6, length(value) - 5)"))

    strippedDF
      .select(
        from_avro($"avro_value", avroSchema).as("data"),
        $"timestamp".as("kafka_timestamp"),
        $"partition",
        $"offset"
      )
      .select(
        $"data.timeObserved",
        $"data.stationId",
        $"data.stationName",
        $"data.metric",
        $"data.value",
        $"data.producer_ts",
        $"kafka_timestamp",
        $"partition",
        $"offset"
      )
      .withColumn("timestamp", to_timestamp($"timeObserved"))

  def createWindowedAggregation(
                                 spark: SparkSession,
                                 streams: Seq[DataFrame]
                               ): DataFrame =
    import spark.implicits.*

    val unionStream = streams.reduce(_ union _)

    unionStream
      .withColumn("processing_time", current_timestamp())
      .groupBy(
        window($"processing_time", "10 minutes"),
        $"metric",
        $"stationId",
        $"stationName"
      )
      .agg(
        avg("value").as("avg_value"),
        min("value").as("min_value"),
        max("value").as("max_value"),
        count("*").as("message_count"),
        min("producer_ts").as("min_producer_ts")
      )
      .select(
        $"window.start".cast(StringType).as("window_start"),
        $"window.end".cast(StringType).as("window_end"),
        $"metric",
        $"stationId",
        $"stationName",
        round($"avg_value", 2).as("avg_value"),
        round($"min_value", 2).as("min_value"),
        round($"max_value", 2).as("max_value"),
        $"message_count",
        $"min_producer_ts",
        (unix_timestamp(current_timestamp()) * 1000).as("processing_end_ts")
      )

  def cleanupCheckpoint(path: String): Unit =
    try
      val dir = new File(path)
      if dir.exists() then
        def deleteRecursively(file: File): Unit =
          if file.isDirectory then
            file.listFiles().foreach(deleteRecursively)
          file.delete()

        deleteRecursively(dir)
        println(s"🧹 Cleaned old checkpoint: $path")
      else
        println(s"✅ No existing checkpoint at: $path")
    catch
      case e: Exception =>
        println(s"⚠️  Warning: Could not clean checkpoint: ${e.getMessage}")

  def monitorProgress(query: StreamingQuery): Unit =
    new Thread(() => {
      Thread.sleep(10000)

      while query.isActive do
        Thread.sleep(30000)

        println(s"\n${"=" * 60}")
        println("📊 STREAMING PROGRESS REPORT")
        println(s"${"=" * 60}")

        if query.isActive then
          val progress = query.lastProgress

          if progress != null then
            println(s"\n🔹 Query: ${query.name}")
            println(s"   Status: ${query.status.message}")
            println(f"   Input rows: ${progress.numInputRows}")
            println(f"   Processing rate: ${progress.processedRowsPerSecond}%.2f rows/s")
            println(f"   Batch duration: ${progress.batchDuration} ms")

            if progress.sources.nonEmpty then
              println("   Sources:")
              progress.sources.foreach { source =>
                println(f"     • ${source.description}: ${source.numInputRows} rows")
              }

        println(s"\n${"=" * 60}\n")

    }).start()