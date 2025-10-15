package ua.playground.benchmark

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.Schema
import io.confluent.kafka.serializers.KafkaAvroSerializer
import java.util.Properties

/**
 * Simple test producer to verify Avro serialization
 * Run this to test if producer_ts is correctly set
 */
object SimpleTestProducer:

  def main(args: Array[String]): Unit =
    println("=== Simple Test Producer ===")

    val producer = createProducer()
    val schema = createAvroSchema()

    try
      // Send 5 test messages
      for i <- 1 to 5 do
        val producerTimestamp = System.currentTimeMillis()

        val record = new GenericData.Record(schema)
        record.put("timeObserved", "2025-01-01T00:00:00")
        record.put("stationId", 1)
        record.put("stationName", "TestStation")
        record.put("metric", "wind_speed")
        record.put("value", 10.5)
        record.put("producer_ts", producerTimestamp)

        val producerRecord = new ProducerRecord[String, GenericRecord](
          "weather.wind",
          s"test-$i",
          record
        )

        producer.send(producerRecord).get()  // Synchronous send

        println(s"✅ Sent message $i with producer_ts: $producerTimestamp")
        Thread.sleep(100)

      producer.flush()
      println("\n✅ All test messages sent successfully")

    finally
      producer.close()

  def createProducer(): KafkaProducer[String, GenericRecord] =
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    props.put("schema.registry.url", "http://localhost:8081")
    props.put(ProducerConfig.ACKS_CONFIG, "1")

    new KafkaProducer[String, GenericRecord](props)

  def createAvroSchema(): Schema =
    val schemaString = """
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
    new Schema.Parser().parse(schemaString)