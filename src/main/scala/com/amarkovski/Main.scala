package com.amarkovski

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val config: Config = ConfigFactory.load("application.conf")

  val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
  import spark.implicits._

  val df = spark.read.option("header","true").option("inferSchema","true").csv(config.getString("paths.ual500"))
  df.printSchema()
  val df_results = df.filter($"anime_id" < 10)
  df_results.coalesce(1).write.mode("overwrite")
    .format("com.databricks.spark.csv").option("header", "true")
    .save(config.getString("paths.results"))

}
