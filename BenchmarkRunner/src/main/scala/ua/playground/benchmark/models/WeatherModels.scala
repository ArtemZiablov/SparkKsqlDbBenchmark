package ua.playground.benchmark

package object models:

  // ===== INPUT MODELS (из CSV) =====

  /** Данные о ветре из CSV файла */
  case class WindData(
                       timeObserved: String,
                       stationId: Int,
                       stationName: String,
                       mean_wind_speed: Double
                     )

  /** Данные о солнечном свете из CSV файла */
  case class SunshineData(
                           timeObserved: String,
                           stationId: Int,
                           stationName: String,
                           bright_sunshine: Double
                         )

  // ===== KAFKA MODELS (Avro сериализация) =====

  /**
   * Унифицированная модель для погодных данных
   * Используется для отправки в Kafka и обработки в Spark
   */
  case class WeatherData(
                          timeObserved: String,
                          stationId: Int,
                          stationName: String,
                          metric: String,        // "wind_speed" или "sunshine"
                          value: Double,
                          producer_ts: Long      // ✅ FIXED: Added timestamp
                        )

  // ===== OUTPUT MODELS (агрегированные результаты) =====

  /**
   * Агрегированные данные по временным окнам
   * ✅ FIXED: Added latency tracking fields
   */
  case class WeatherAggregation(
                                 windowStart: String,
                                 windowEnd: String,
                                 metric: String,
                                 stationId: Int,
                                 stationName: String,
                                 avgValue: Double,
                                 minValue: Double,
                                 maxValue: Double,
                                 messageCount: Long,
                                 minProducerTs: Long,       // ✅ FIXED: Earliest producer timestamp in window
                                 processingEndTs: Long      // ✅ FIXED: When aggregation completed
                               )

  // ===== BENCHMARK MODELS =====

  /** Метрики производительности бенчмарка */
  case class BenchmarkMetrics(
                               throughputMsgPerSec: Int,
                               avgLatencyMs: Double,
                               p95LatencyMs: Double,
                               p99LatencyMs: Double,
                               avgCpuUsagePercent: Double,
                               avgMemoryUsageMB: Double,
                               messagesProcessed: Long,
                               testDurationSeconds: Long
                             )

  /** Результат одного теста */
  case class TestResult(
                         testName: String,
                         targetThroughput: Int,
                         metrics: BenchmarkMetrics,
                         timestamp: String
                       )