name := "flink-cdc-pipeline-connector-phaker"
idePackagePrefix := Some("io.github.yuxiqian.phaker")
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.19"

val flinkVersion = "1.18.1"
val flinkCdcVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-runtime" % flinkVersion % "provided",
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % "provided",
  "org.apache.flink" % "flink-cdc-common" % flinkCdcVersion,
  "org.apache.flink" % "flink-cdc-runtime" % flinkCdcVersion,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.apache.flink" % "flink-clients" % flinkVersion % Test,
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % Test
)
