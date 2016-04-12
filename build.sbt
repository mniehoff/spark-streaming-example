lazy val sparkStreamingExample = project
  .copy(id = "spark-streaming-example")
  .in(file("."))
  .enablePlugins(AutomateHeaderPlugin, GitVersioning)

name := "spark-streaming-example"

libraryDependencies ++= Vector(
  Library.scalaCheck % "test",
  Library.spark,
  Library.sparkKafka,
  Library.sparkStreaming,
  Library.sparkCassandra
)

initialCommands := """|import de.codecentric.spark.streaming.example._
                      |""".stripMargin


mergeStrategy in assembly := {
  case PathList("META-INF", "ECLIPSEF.RSA") => MergeStrategy.discard
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

scalaVersion := Version.Scala
