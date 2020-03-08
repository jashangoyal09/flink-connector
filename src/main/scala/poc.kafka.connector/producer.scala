package poc.kafka.connector

import java.lang
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, FlinkKafkaProducer011, FlinkKafkaProducerBase}

case class Person(name:String)
object producer extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val stream =env.readTextFile("file:///home/knoldus/Downloads/wall_hits.csv")
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  // only required for Kafka 0.8
  properties.setProperty("zookeeper.connect", "localhost:2181")
  properties.setProperty("group.id", "test")

//  val myProducer = new FlinkKafkaProducer011[String](
//    "localhost:9092",         // broker list
//    "my-topic",               // target topic
//    new SimpleStringSchema)
//
val myProducer = new FlinkKafkaProducer[String](
  "localhost:9092",         // broker list
  "topic",               // target topic
  new SimpleStringSchema)

  myProducer.setWriteTimestampToKafka(true)

//  stream.print()
  stream.addSink(myProducer)
  env.execute("produce data to the topic")
}
