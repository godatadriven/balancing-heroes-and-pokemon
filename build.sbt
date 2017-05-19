resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal)

name := "Balancing heroes and pokemon"

version := "0.1-SNAPSHOT"

organization := "com.godatadriven"

scalaVersion in ThisBuild := "2.11.11"

val flinkVersion = "1.3.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",

  "org.json4s" %% "json4s-native" % "3.5.2",
  "org.json4s" %% "json4s-jackson" % "3.5.2",

  "ch.qos.logback" % "logback-classic" % "1.0.13",

  "org.scalanlp" %% "breeze" % "0.13.1",

  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)