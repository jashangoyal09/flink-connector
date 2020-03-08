name := "flink-kafka"

version := "0.1"

scalaVersion := "2.12.10"
val flinkVersion = "1.10.0"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  // https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kinesis
  "org.apache.flink" %% "flink-connector-kinesis" % "1.10.0",
  "org.apache.flink" %% "flink-connector-kafka" % "1.10.0"
)