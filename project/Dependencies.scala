import sbt._

object Version {
  final val Scala      = "2.10.6"
  final val ScalaCheck = "1.12.5"
  final val Spark      = "1.6.1"
  final val SparkCassandra = "1.5.0-RC1"
}

object Library {
  val scalaCheck      = "org.scalacheck"      %% "scalacheck"                 % Version.ScalaCheck
  val spark           = "org.apache.spark"    %% "spark-core"                 % Version.Spark
  val sparkStreaming  = "org.apache.spark"    %% "spark-streaming"            % Version.Spark
  val sparkKafka      = "org.apache.spark"    %% "spark-streaming-kafka"      % Version.Spark
  val sparkCassandra  = "com.datastax.spark"  %% "spark-cassandra-connector"  % Version.SparkCassandra
}
