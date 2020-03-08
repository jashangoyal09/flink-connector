package poc.kafka.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants

object kinesisProducer extends App {

  val producerConfig = new Properties()
  // Required configs
  producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
  producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "123")
  producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "xyz")
  // Optional KPL configs
  producerConfig.put("AggregationMaxCount", "4294967295")
  producerConfig.put("CollectionMaxCount", "1000")
  producerConfig.put("RecordTtl", "30000")
  producerConfig.put("RequestTimeout", "6000")
  producerConfig.put("ThreadPoolSize", "15")

  // Disable Aggregation if it's not supported by a consumer
  // producerConfig.put("AggregationEnabled", "false")
  // Switch KinesisProducer's threading model
  // producerConfig.put("ThreadingModel", "PER_REQUEST")

  val propertieConsumer = new Properties()
  propertieConsumer.setProperty("bootstrap.servers", "localhost:9092")
  // only required for Kafka 0.8
  propertieConsumer.setProperty("zookeeper.connect", "localhost:2181")

  val kinesis = new FlinkKinesisProducer[String](new SimpleStringSchema, producerConfig)
  kinesis.setFailOnError(true)
  kinesis.setDefaultStream("kinesis_stream_name")
  kinesis.setDefaultPartition("0")
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream = env.addSource(new FlinkKafkaConsumer[String]("topic", new SimpleStringSchema(), propertieConsumer))

  stream.print
  val myProducer = new FlinkKafkaProducer[String](
    "localhost:9092",         // broker list
    "topic123",               // target topic
    new SimpleStringSchema)

  myProducer.setWriteTimestampToKafka(true)
  stream.addSink(myProducer)
  env.execute("kinesis producer to publish in kafka topic")
}
