package com.amarkovski

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
//import com.amarkovski.Queries

object Main extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val logger = Logger.getLogger(getClass.getName)
  logger.warn("Main Program Started v024")

  val config: Config = ConfigFactory.load("application.conf")

  val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
  val conf = new SparkConf().setAppName("AWS_Antoni")
  //val sc = new SparkContext(conf)
  import spark.implicits._

  /*val spark = SparkSession .builder()
    .appName("AWS_Antoni")
    .config("spark.sql.warehouse.dir", "hdfs://masternode-10-143-30-198.eu-west-1.compute.internal:8020/user/amarkovski/hive")
    .enableHiveSupport()
    .getOrCreate()*/

  //spark.sql("create database if not exists amarkovskidb")
  //val df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://masternode-10-143-30-198.eu-west-1.compute.internal:8020/user/amarkovski/datasets/AnimeList.csv")
  val df = spark.read.option("header", "true").option("inferSchema", "true").csv(config.getString("paths.d_ual"))
  val df_ul = spark.read.option("header", "true").option("inferSchema", "true").csv(config.getString("paths.d_ul"))
  val df_reduced = df.select("username","anime_id","my_watched_episodes","my_score","my_tags").filter("length(username) > 1" )
  //df.write.mode("overwrite").saveAsTable("amarkovskidb.animelist")
  df_reduced.createOrReplaceTempView("ual_temp")
  //df_reduced.coalesce(1).write.mode("overwrite")    .format("com.databricks.spark.csv").option("header", "true").save("resources/datasets/d_ual_5c")
  val test = spark.sql("select username from ual_temp group by username limit 10").show()

  val q = new Queries
  q.mostWatchedByUser(df_reduced).show()
  //q.mostTimeSpentByUser(df_ul).show
  //q.mostEpisodesByUser(df_reduced).show
  //q.usersCountFromUl(df_ul).show
  //q.usersCountFromUal(df_reduced).show
  q.highestAvgRatingByUser(df_reduced).show
  //val queries = new Queries

  //val df1_1 = queries.animCount(df)



  //print tables

  //print value from a table

  //create table and write table from userlist table of 500 lines +

  logger.warn("Main Program END")



}
