// build.sbt for the proto_row_generator project

// Specify the Scala version compatible with Spark 3.2.1
ThisBuild / scalaVersion := "2.12.18"

// Define the root project settings
lazy val root = (project in file(".")).settings(
  name := "proto-row-generator",
  // Spark 3.2.1 depends on Scala 2.12, bring in spark‑sql as a dependency
  libraryDependencies ++= Seq(
    // Apache Spark SQL provides the catalyst InternalRow and UnsafeProjection APIs
    "org.apache.spark" %% "spark-sql" % "3.2.1",
    // Protobuf runtime for compiled message classes and descriptors
    "com.google.protobuf" % "protobuf-java" % "3.25.2",
    // Janino is used to compile Java code on the fly
    "org.codehaus.janino" % "janino" % "3.1.9"
  ),
  // Emit deprecation warnings for easier maintenance
  scalacOptions ++= Seq("-deprecation")
)