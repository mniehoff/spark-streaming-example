lazy val sparkStreamingExample = project
  .copy(id = "spark-streaming-example")
  .in(file("."))
  .enablePlugins(AutomateHeaderPlugin, GitVersioning)

name := "spark-streaming-example"

libraryDependencies ++= Vector(
  Library.scalaCheck % "test"
)

initialCommands := """|import de.codecentric.spark.streaming.example._
                      |""".stripMargin
