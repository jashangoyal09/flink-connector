package poc.kafka.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}

object kinesisConsumer extends App {

  val consumerConfig = new Properties()
  consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
  consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "foobar")
  consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "foobar")
  consumerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4568")
  consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")

  val env1 = StreamExecutionEnvironment.getExecutionEnvironment
  env1.setParallelism(5)

  val kinesis = env1.addSource(new FlinkKinesisConsumer[String](
    "kinesis_stream_name1", new SimpleStringSchema, consumerConfig)).print()

  env1.execute("kinesis consumer")
}
