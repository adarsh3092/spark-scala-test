ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "spark-scala-test"
  )

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.1.1_1.2.0" % Test

javaOptions in Test ++= Seq("-Dproject.dir=test")

// Configurations to speed up tests and reduce memory footprint
javaOptions in Test ++= Seq(
  "-Dspark.ui.enabled=false",
  "-Dspark.ui.showConsoleProgress=false",
  "-Dspark.sql.shuffle.partitions=5",
  "-Ddelta.log.cacheSize=3",
  "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
  "-Dspark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
  "-Xmx1024m"
)