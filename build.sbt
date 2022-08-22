ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "kstreams-left-join-example",
    idePackagePrefix := Some("com.gjosquin.example.kstreams"),
    resolvers += "Confluent Maven Repository".at("https://packages.confluent.io/maven/"),
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka-streams-scala" % "3.2.1",
      "org.scalatest" %% "scalatest" % "3.2.12" % Test,
      "org.apache.kafka" % "kafka-streams-test-utils" % "3.2.1" % Test
    )
  )
