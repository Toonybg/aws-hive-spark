import com.amarkovski.Queries
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class QueriesTest extends AnyFunSuite {

  import org.apache.log4j.PropertyConfigurator

  //PropertyConfigurator.configure(getClass.getResource("resources/log4j.properties"))

  //2. Pour chaque utilisateur, le nombre d'Animés différents qu'il a regardé
  val homePath = "resources/datasets/"
  val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
  val q = Queries()
  val df_d_ual = spark.read.option("header", "true").option("inferSchema", "true").csv(homePath + "d_UserAnimeList.csv")
  Logger.getLogger("org").setLevel(Level.WARN)


  test("Queries.animCount") {
    val line1 = q.animCount(df_d_ual).collect.apply(0)
    val row = Row("RedvelvetDaisuki",702)
    assert( row === line1)
  }

}
