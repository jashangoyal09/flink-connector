package poc.kafka.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object consumer extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
//  env.setParallelism(1)

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  // only required for Kafka 0.8
  properties.setProperty("zookeeper.connect", "localhost:2181")
//  properties.setProperty("group.id", "test")
val stream = env

  .addSource(new FlinkKafkaConsumer[String]("topic", new SimpleStringSchema(), properties))

  stream.print()
  env.execute("consume data from topic")
}
