name := "calculation_distance"
version := "1.0"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
"org.apache.spark" % "spark-core_2.11" % "2.3.0" % "provided",
"org.apache.spark" % "spark-sql_2.11" % "2.3.0" % "provided",
"org.apache.spark" % "spark-hive_2.11" % "2.3.0" % "provided",
"org.apache.lucene" % "lucene-queries" % "8.2.0"
)
