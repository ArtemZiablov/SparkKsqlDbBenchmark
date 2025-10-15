package ua.playground.benchmark

import ua.playground.benchmark.models.*
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{StringDeserializer, ByteArrayDeserializer}
import org.apache.avro.{Schema, generic}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import scala.jdk.CollectionConverters.*
import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.math.{sqrt, pow}

object LatencyMonitor:

  def main(args: Array[String]): Unit =
    println("=" * 70)
    println(" 📊 LATENCY MONITOR - Streaming Benchmark Analysis")
    println("=" * 70)

    val throughput = if args.length > 0 then
      args(0).toInt
    else
      throw new IllegalArgumentException("Target throughput (msg/s) required as first argument")

    val config = loadConfig()

    println(s" Target Throughput: $throughput msg/s")
    println(s" Input Topic: ${config.inputTopic}")
    println(s" Kafka Brokers: ${config.kafkaBootstrapServers}")
    println("=" * 70)
    println()

    try
      val dataPoints = collectLatencyData(config)

      if dataPoints.isEmpty then
        println("⚠️  WARNING: No latency data found!")
        println("   Make sure:")
        println("   1. Producer has sent messages")
        println("   2. Consumer has processed and written results")
        println("   3. Output topic exists and has data")
        System.exit(1)

      val metrics = calculateMetrics(dataPoints, throughput)
      val report = createReport(throughput, metrics, dataPoints)
      printReport(report)

      if config.exportToPrometheus then
        MetricsExporter.exportToPrometheus(metrics, config.prometheusPort)

      saveReport(report)

      println("\n✅ Latency monitoring completed successfully!")
      System.exit(0)

    catch
      case e: Exception =>
        println(s"\n❌ ERROR: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)

  def loadConfig(): MonitorConfig =
    MonitorConfig(
      kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      schemaRegistryUrl = sys.env.getOrElse("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
      inputTopic = sys.env.getOrElse("INPUT_TOPIC", "weather.aggregated.output"),
      groupId = s"latency-monitor-${System.currentTimeMillis()}",
      maxWaitTimeMs = sys.env.getOrElse("MAX_WAIT_TIME_MS", "30000").toInt,
      maxEmptyPolls = sys.env.getOrElse("MAX_EMPTY_POLLS", "5").toInt,
      exportToPrometheus = sys.env.getOrElse("EXPORT_TO_PROMETHEUS", "false").toBoolean,
      prometheusPort = sys.env.getOrElse("PROMETHEUS_PORT", "9090").toInt
    )

  def collectLatencyData(config: MonitorConfig): Seq[LatencyDataPoint] =
    val consumer = createConsumer(config)
    consumer.subscribe(List(config.inputTopic).asJava)

    // ✅ Create Avro schema matching Spark's output EXACTLY
    val schemaString = """
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

    val schema = new Schema.Parser().parse(schemaString)
    val datumReader = new GenericDatumReader[GenericRecord](schema)

    val dataPoints = scala.collection.mutable.ListBuffer[LatencyDataPoint]()
    val startTime = System.currentTimeMillis()

    println(s"📊 Reading latency data from topic: ${config.inputTopic}")
    println(s"⏱️  Max wait time: ${config.maxWaitTimeMs / 1000} seconds")
    println(s"⏱️  Max empty polls: ${config.maxEmptyPolls}")
    println()

    var recordCount = 0
    var emptyPollCount = 0
    var firstRecord = true

    try
      while (System.currentTimeMillis() - startTime) < config.maxWaitTimeMs
        && emptyPollCount < config.maxEmptyPolls do

        val records = consumer.poll(Duration.ofMillis(1000))

        if records.isEmpty then
          emptyPollCount += 1
          print(".")
        else
          emptyPollCount = 0

          records.asScala.foreach { record =>
            try
              // ✅ Manually deserialize raw Avro bytes from Spark
              val bytes = record.value()

              if firstRecord then
                println(s"\n🔍 DEBUG: Received ${bytes.length} bytes")
                println(s"🔍 DEBUG: First 20 bytes (hex): ${bytes.take(20).map(b => f"$b%02x").mkString(" ")}")
                firstRecord = false

              val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
              val avroRecord = datumReader.read(null, decoder)

              // Debug first record
              if recordCount == 0 then
                println(s"🔍 DEBUG: Successfully decoded first record")
                println(s"🔍 DEBUG: Schema fields: ${avroRecord.getSchema.getFields.asScala.map(_.name).mkString(", ")}")

              // Extract fields
              val windowStart = avroRecord.get("window_start").toString
              val windowEnd = avroRecord.get("window_end").toString
              val metric = avroRecord.get("metric").toString
              val stationId = avroRecord.get("stationId").asInstanceOf[Int]
              val stationName = avroRecord.get("stationName").toString
              val messageCount = avroRecord.get("message_count").asInstanceOf[Long]

              // ✅ CRITICAL: Extract timestamps with proper type handling
              val minProducerTsRaw = avroRecord.get("min_producer_ts")
              val processingEndTsRaw = avroRecord.get("processing_end_ts")

              // Debug timestamps
              if recordCount == 0 then
                println(s"🔍 DEBUG: min_producer_ts raw = $minProducerTsRaw (type: ${minProducerTsRaw.getClass.getName})")
                println(s"🔍 DEBUG: processing_end_ts raw = $processingEndTsRaw (type: ${processingEndTsRaw.getClass.getName})")

              val minProducerTs = minProducerTsRaw.asInstanceOf[Long]
              val processingEndTs = processingEndTsRaw.asInstanceOf[Long]

              if recordCount == 0 then
                println(s"🔍 DEBUG: min_producer_ts = $minProducerTs")
                println(s"🔍 DEBUG: processing_end_ts = $processingEndTs")
                println()

              // Calculate latency
              val latencyMs = (processingEndTs - minProducerTs).toDouble

              // Sanity check: latency should be positive and reasonable (< 60 seconds)
              if latencyMs > 0 && latencyMs < 60000 then
                val dataPoint = LatencyDataPoint(
                  windowStart = windowStart,
                  windowEnd = windowEnd,
                  metric = metric,
                  stationId = stationId,
                  stationName = stationName,
                  messageCount = messageCount,
                  minProducerTs = minProducerTs,
                  processingEndTs = processingEndTs,
                  latencyMs = latencyMs
                )

                dataPoints += dataPoint
                recordCount += 1

                if recordCount % 10 == 0 then
                  print(s"\r   Records processed: $recordCount")
              else
                println(s"\n⚠️  Skipping invalid latency: ${latencyMs}ms (producer=${minProducerTs}, processing=${processingEndTs})")

            catch
              case e: ClassCastException =>
                println(s"\n⚠️  Type casting error: ${e.getMessage}")
                e.printStackTrace()
              case e: Exception =>
                println(s"\n⚠️  Error processing record: ${e.getMessage}")
                e.printStackTrace()
          }

      println(s"\n\n✅ Collected ${dataPoints.size} latency data points")
      println(s"   Records processed: $recordCount")
      println(s"   Duration: ${(System.currentTimeMillis() - startTime) / 1000.0}%.2f seconds")
      println()

      dataPoints.toSeq

    finally
      consumer.close()

  def calculateMetrics(dataPoints: Seq[LatencyDataPoint], throughput: Int): LatencyMetrics =
    require(dataPoints.nonEmpty, "No data points to calculate metrics")

    val latencies = dataPoints.map(_.latencyMs).sorted
    val count = latencies.size
    val sum = latencies.sum
    val avg = sum / count
    val min = latencies.head
    val max = latencies.last

    def percentile(p: Double): Double =
      val index = (count * p).toInt
      latencies(Math.min(index, count - 1))

    val p50 = percentile(0.50)
    val p95 = percentile(0.95)
    val p99 = percentile(0.99)

    val variance = latencies.map(l => pow(l - avg, 2)).sum / count
    val stdDev = sqrt(variance)

    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

    LatencyMetrics(
      avgLatencyMs = avg,
      p50LatencyMs = p50,
      p95LatencyMs = p95,
      p99LatencyMs = p99,
      minLatencyMs = min,
      maxLatencyMs = max,
      stdDevLatencyMs = stdDev,
      sampleCount = count,
      testThroughput = throughput,
      timestamp = timestamp
    )

  def createReport(
                    throughput: Int,
                    metrics: LatencyMetrics,
                    dataPoints: Seq[LatencyDataPoint]
                  ): LatencyReport =
    val testName = s"Latency-Test-${throughput}msg-s"

    val summary = s"""
                     |Latency Test Summary
                     |-------------------
                     |Test: $testName
                     |Target Throughput: $throughput msg/s
                     |Sample Count: ${metrics.sampleCount}
                     |
                     |Latency Statistics:
                     |  Average: ${metrics.avgLatencyMs}%.2f ms
                     |  Median (P50): ${metrics.p50LatencyMs}%.2f ms
                     |  P95: ${metrics.p95LatencyMs}%.2f ms
                     |  P99: ${metrics.p99LatencyMs}%.2f ms
                     |  Min: ${metrics.minLatencyMs}%.2f ms
                     |  Max: ${metrics.maxLatencyMs}%.2f ms
                     |  Std Dev: ${metrics.stdDevLatencyMs}%.2f ms
    """.stripMargin

    LatencyReport(
      testName = testName,
      targetThroughput = throughput,
      metrics = metrics,
      dataPoints = dataPoints,
      summary = summary
    )

  def printReport(report: LatencyReport): Unit =
    println("\n" + "=" * 70)
    println(s" 📊 LATENCY TEST RESULTS: ${report.testName}")
    println("=" * 70)
    println(f" Target Throughput:    ${report.targetThroughput}%,d msg/s")
    println(f" Sample Count:         ${report.metrics.sampleCount}%,d windows")
    println(f" Timestamp:            ${report.metrics.timestamp}")
    println("-" * 70)
    println(" Latency Statistics:")
    println("-" * 70)
    println(f"   Average (Mean):     ${report.metrics.avgLatencyMs}%8.2f ms")
    println(f"   Median (P50):       ${report.metrics.p50LatencyMs}%8.2f ms")
    println(f"   P95:                ${report.metrics.p95LatencyMs}%8.2f ms")
    println(f"   P99:                ${report.metrics.p99LatencyMs}%8.2f ms")
    println(f"   Min:                ${report.metrics.minLatencyMs}%8.2f ms")
    println(f"   Max:                ${report.metrics.maxLatencyMs}%8.2f ms")
    println(f"   Std Deviation:      ${report.metrics.stdDevLatencyMs}%8.2f ms")
    println("-" * 70)
    println(" Analysis:")
    println("-" * 70)

    if report.metrics.avgLatencyMs < 30 then
      println("   ✅ EXCELLENT - Very low average latency")
    else if report.metrics.avgLatencyMs < 50 then
      println("   ✅ GOOD - Low average latency")
    else if report.metrics.avgLatencyMs < 100 then
      println("   ⚠️  MODERATE - Acceptable latency")
    else
      println("   ❌ HIGH - Latency needs optimization")

    if report.metrics.p99LatencyMs < 50 then
      println("   ✅ EXCELLENT - P99 latency very low")
    else if report.metrics.p99LatencyMs < 100 then
      println("   ✅ GOOD - P99 latency acceptable")
    else if report.metrics.p99LatencyMs < 200 then
      println("   ⚠️  MODERATE - P99 latency high")
    else
      println("   ❌ HIGH - P99 latency needs attention")

    if report.metrics.stdDevLatencyMs < 10 then
      println("   ✅ EXCELLENT - Very consistent latency")
    else if report.metrics.stdDevLatencyMs < 20 then
      println("   ✅ GOOD - Consistent latency")
    else
      println("   ⚠️  MODERATE - Some variance in latency")

    val p95ToP99Ratio = report.metrics.p99LatencyMs / report.metrics.p95LatencyMs
    if p95ToP99Ratio > 1.5 then
      println(f"   ⚠️  Long tail detected (P99/P95 ratio: ${p95ToP99Ratio}%.2f)")

    println("=" * 70)

    println("\n 🔍 Top 5 Highest Latency Windows:")
    println("-" * 70)
    report.dataPoints
      .sortBy(-_.latencyMs)
      .take(5)
      .zipWithIndex
      .foreach { case (dp, idx) =>
        println(f"   ${idx + 1}. ${dp.metric}%-12s @ ${dp.stationName}%-20s: ${dp.latencyMs}%7.2f ms")
        println(f"      Window: ${dp.windowStart} - ${dp.windowEnd}")
      }
    println()

  def saveReport(report: LatencyReport): Unit =
    import java.io.PrintWriter
    import java.io.File

    val outputDir = sys.env.getOrElse("OUTPUT_DIR", "./benchmark-results")
    new File(outputDir).mkdirs()

    val filename = s"$outputDir/latency-report-${report.targetThroughput}msg-s-${System.currentTimeMillis()}.txt"

    val writer = new PrintWriter(new File(filename))
    try
      writer.write(report.summary)
      writer.write("\n\n")
      writer.write("=" * 70 + "\n")
      writer.write("Detailed Data Points:\n")
      writer.write("=" * 70 + "\n")
      report.dataPoints.foreach { dp =>
        writer.write(f"${dp.windowStart}%-25s ${dp.metric}%-12s ${dp.stationName}%-20s ${dp.latencyMs}%7.2f ms\n")
      }
      println(s"📄 Report saved to: $filename")
    finally
      writer.close()

  def createConsumer(config: MonitorConfig): KafkaConsumer[String, Array[Byte]] =
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")

    new KafkaConsumer[String, Array[Byte]](props)