package com.amarkovski

import org.apache.spark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Queries() {

  val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
  import spark.implicits._

  //methods for queries

  //1
  //2. Pour chaque utilisateur, le nombre d'Animés différents qu'il a regardé
  def animCount() = {
    spark.sql("SELECT username, count(distinct(anime_id)) FROM AnimUserList GROUP BY username")
  }
  //1. Pour chaque utilisateur, le nombre d'Animés différents qu'il a regardé
  def animCount(dataFrame: DataFrame) = {
      dataFrame.select("username").filter("length(username) > 1" ).filter("username is not null")
      .groupBy("username")
      .count().orderBy($"count".desc)
  }

  //2. Pour chaque utilisateur, l'Animé qu'il a le plus regardé
  // username, anime_id, my_watched_episodes
  def mostWatchedByUser(ual_raw: DataFrame):DataFrame = {
    val ual = ual_raw.select("username","anime_id","my_watched_episodes").filter("length(username) > 1" )
      .filter("anime_id is not null").filter("my_watched_episodes is not null")
      .filter("my_watched_episodes < 60000")
    val w2 = Window.partitionBy("username").orderBy($"my_watched_episodes".desc)
    ual.withColumn("row",row_number.over(w2)).where($"row" === 1).drop("row").orderBy($"my_watched_episodes".desc)
  }
  // 5 L'utilisateur qui a regardé le plus d’Animés
  def mostTimeSpentByUser(ul_raw: DataFrame) = {
    val ul = ul_raw.select("username","user_days_spent_watching").filter("length(username) > 1" )
    ul.orderBy($"user_days_spent_watching".desc)
  }
  //—6 L'utilisateur qui a regardé le plus d’épisodes #my_watched_episodes
  def mostEpisodesByUser(ual_raw: DataFrame) = {
    val ual = ual_raw.select("username", "my_watched_episodes").filter("length(username) > 1").filter("my_watched_episodes is not null")
    ual.groupBy("username").sum("my_watched_episodes").orderBy($"sum(my_watched_episodes)".desc).toDF("username","sum_my_watched_episodes")
  }

  //—7 Combien y a-t-il d'utilisateurs différents? #30267 5
  def usersCountFromUl(ul_raw: DataFrame) = {
    val ul = ul_raw.select("username")
    ul.select(countDistinct("username")).toDF("count_distinct_username")
  }
  def usersCountFromUal(ual: DataFrame) = {
    ual.select(countDistinct("username"))
  }

  def highestAvgRatingByUser(ual: DataFrame) = {
    ual.groupBy("username").avg("my_score").orderBy($"avg(my_score)".desc).toDF("username","avg_my_score").limit(100)
  }

  def saveDf(df: DataFrame,path: String): Unit = {
    df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(path)

  }
}
