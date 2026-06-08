import sbtassembly.AssemblyPlugin.autoImport.*

ThisBuild / scalaVersion := "2.13.17"

ThisBuild / version := "0.1.0-SNAPSHOT"

val catsEffectVersion = "3.5.7"
val fs2Version        = "3.12.0"
val fs2KafkaVersion   = "3.9.1"
val circeVersion      = "0.14.12"
val cirisVersion      = "3.11.0"
val log4catsVersion   = "2.7.1"
val logbackClassic = "1.5.18"
val logbackLogstashEncoder = "8.0"

lazy val root = (project in file("."))
  .settings(
    name := "scala-data-pipeline",

    libraryDependencies ++= Seq(

      // Cats Effect
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "org.typelevel" %% "cats-effect-kernel" % catsEffectVersion,

      // FS2
      "co.fs2" %% "fs2-core" % fs2Version,

      // Kafka
      "com.github.fd4s" %% "fs2-kafka" % fs2KafkaVersion,

      // Circe
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,

      // Config
      "is.cir" %% "ciris" % cirisVersion,

      // Logging
      "org.typelevel" %% "log4cats-slf4j" % log4catsVersion,
      "ch.qos.logback" % "logback-classic" % logbackClassic,
      "net.logstash.logback" % "logstash-logback-encoder" % logbackLogstashEncoder
    ),

    // Merge Strategy
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "versions", xs @ _*) if xs.last.endsWith("module-info.class") => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case "module-info.class" => MergeStrategy.discard
      case "META-INF/io.netty.versions.properties" => MergeStrategy.first
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.filterDistinctLines
      case PathList("META-INF", "maven", xs @ _*) => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case "application.conf" => MergeStrategy.concat
      case x => MergeStrategy.first
    },

    // Force explicit naming
    assembly / assemblyJarName := "pipeline-service.jar"

  )