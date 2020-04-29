package com.amarkovski

import com.amarkovski.Main.getClass
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

object Execution {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val logger = Logger.getLogger(getClass.getName)
    logger.warn("*** Execution Program Started v018 ***")

    val homePath = "hdfs://masternode-10-143-30-198.eu-west-1.compute.internal:8020/user/amarkovski/"

    //val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    val conf = new SparkConf().setAppName("AWS_Antoni")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .appName("AWS_Antoni")
      .config("spark.sql.warehouse.dir", "hdfs://masternode-10-143-30-198.eu-west-1.compute.internal:8020/user/amarkovski/hive")
      .enableHiveSupport()
      .getOrCreate()


    import spark.implicits._
    //val saveMode = "overwrite"
    val saveMode = "ignore"
    val df_d_ual = spark.read.option("header", "true").option("inferSchema", "true").csv(homePath + "datasets/d_UserAnimeList.csv")
    df_d_ual.write.mode(saveMode).saveAsTable("amarkovskidb.d_useranimelist")

    val df_ual = spark.read.option("header", "true").schema(df_d_ual.schema).csv(homePath + "datasets/UserAnimeList.csv")
    df_ual.write.mode(saveMode).saveAsTable("amarkovskidb.useranimelist")

    val df_al = spark.read.option("header", "true").option("inferSchema", "true").csv(homePath + "datasets/AnimeList.csv")
    df_al.write.mode(saveMode).saveAsTable("amarkovskidb.animelist")

    val df_ul = spark.read.option("header", "true").option("inferSchema", "true").csv(homePath + "datasets/UserList.csv")
    df_ul.write.mode(saveMode).saveAsTable("amarkovskidb.userlist")

    spark.sql("use amarkovskidb").show()
    spark.sql("show tables").show()


    try {
      spark.sql("show tables").show()
      spark.sql("select count(*) from useranimelist").show() //80298436
      spark.sql("select count(*) from userlist").show() //302676
      spark.sql("select count(*) from animelist").show() //14478
    }
    catch {case e: NoSuchTableException => logger.warn("no such table exeption \n" + e.printStackTrace())
    }

    logger.warn("*** Execution Program END ***")

    spark.stop()

  }

}