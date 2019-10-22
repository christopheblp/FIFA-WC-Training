name := "99Problems"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.4"
val kafkaVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion
)
