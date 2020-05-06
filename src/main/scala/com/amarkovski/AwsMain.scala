package com.amarkovski

import com.amarkovski.Execution.getClass
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object AwsMain {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val logger = Logger.getLogger(getClass.getName)
    logger.warn("*** AwsMain Program Started v030 ***")

    val homePath = "hdfs://masternode-10-143-30-198.eu-west-1.compute.internal:8020/user/amarkovski/"
    val mydb = "amarkovskidb"
    //val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    val conf = new SparkConf().setAppName("AWS_Antoni")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .appName("AWS_Antoni")
      .config("spark.sql.warehouse.dir", "hdfs://masternode-10-143-30-198.eu-west-1.compute.internal:8020/user/amarkovski/hive")
      .enableHiveSupport()
      .getOrCreate()

    //val al = spark.sqlContext.table(mydb + ".animelist")
    val ul = spark.sqlContext.table(mydb + ".userlist").filter("length(username) > 1 and username is not null" )
    val ual = spark.sqlContext.table(mydb + ".useranimelist").filter("length(username) > 1 and username is not null" )



    import spark.implicits._
    val q = new Queries
    //req1 Pour chaque utilisateur, le nombre d'Animés différents qu'il a regardé
    val req1 = q.animCount(ual)
    //req1.show()
    req1.write.mode("igrnore").saveAsTable(mydb + ".req1")
    q.saveDf(req1,homePath + "datasets/req1")

    //3. Pour chaque utilisateur, l'Animé qu'il a le plus regardé
    val req2 = q.mostWatchedByUser(ual)
    //req2.show()
    req2.write.mode("ignore").saveAsTable(mydb + ".req2")
    q.saveDf(req2,homePath + "datasets/req2")

    val req3 = q.mostTimeSpentByUser(ul)
    //req3.show()
    req3.write.mode("ignore").saveAsTable(mydb + ".req3")
    q.saveDf(req3,homePath + "datasets/req3")


    // —6 L'utilisateur qui a regardé le plus d’épisodes #my_watched_episodes
    val req4 = q.mostEpisodesByUser(ual)
    //req4.show()
    req4.write.mode("ignore").saveAsTable(mydb + ".req4")
    q.saveDf(req4,homePath + "datasets/req4")

    //—7 Combien y a-t-il d'utilisateurs différents? #30267 5
    val req5 = q.usersCountFromUl(ul)
    //req5.show()
    req5.write.mode("ignore").saveAsTable(mydb + ".req5")
    q.saveDf(req5,homePath + "datasets/req5")

    //—8 Les 100 utilisateurs qui ont donné les moyennes de notes les plus élevées. #my_score
    val req6 = q.highestAvgRatingByUser(ual)
    //req6.show()
    req6.write.mode("ignore").saveAsTable(mydb + ".req6")
    q.saveDf(req6,homePath + "datasets/req6")

    logger.warn("*** AwsMain Program END ***")

    spark.stop()


  }

}
