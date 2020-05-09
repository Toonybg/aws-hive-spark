name := "aws-hive-spark"
version := "0.13.2"
scalaVersion := "2.11.12"
val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  //"org.apache.spark" %% "spark-streaming" % sparkVersion,
  //"org.apache.spark" %% "spark-streaming-twitter" % sparkVersion
  "org.scalactic" %% "scalactic" % "3.1.1",
  "org.scalatest" %% "scalatest" % "3.1.1" % "test",
  "com.typesafe" % "config" % "1.4.0"
)

