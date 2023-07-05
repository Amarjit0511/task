ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "Implementation",
    idePackagePrefix := Some("com.xenonstack.amarjit")
  )

val sparkVersion="3.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "3.5.0",
  "org.apache.kafka" % "kafka-clients" % "3.4.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "io.delta" %% "delta-core" % "2.4.0"

)

