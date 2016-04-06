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

//We do this so that Spark Dependencies will not be bundled with our fat jar but will still be included on the classpath
//When we do a sbt/run
// run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

mergeStrategy in assembly := {
  case PathList("META-INF", "ECLIPSEF.RSA") => MergeStrategy.discard
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

scalaVersion := Version.Scala
