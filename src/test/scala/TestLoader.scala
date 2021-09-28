import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.log4j.{Level, LogManager}
import org.scalatest.funsuite.AnyFunSuite
import com.rostel.{DataLoader, SparkSessionWrapper}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when, lit}


class TestLoader extends AnyFunSuite with SparkSessionWrapper with DatasetComparer {

  import spark.implicits._
  LogManager.getLogger("org").setLevel(Level.ERROR)

  // Define constants
  // Test data !!!
  val test_path_logs: String = "hdfs:/user/sergey/clickstream/test/*tsv"
  val path_categories: String = "hdfs:/user/sergey/categories/categories.tsv"
  val path_mrf: String = "hdfs:/user/sergey/regions/regions.tsv"
  val db_test: String = "rostel_test"

  val raw_logs: DataFrame = DataLoader.loadRawLogs(spark, test_path_logs)
  val logs: DataFrame = DataLoader.parseRawLogs(raw_logs)

  test("Same number of records loaded?") {
    val today_records = logs.count()
    val today_records_expected = spark.table(s"$db_test.logs_mrf_benchmark").count()
    assert(today_records == today_records_expected)
  }

  test("Same first login?") {
    val first_session_testing = DataLoader.makeFirstSession(logs)
    val first_session_expected = spark.sql(s"select * from $db_test.first_session_benchmark")
    assertSmallDatasetEquality(first_session_testing, first_session_expected, orderedComparison = false)
  }

  test("Same most popular category?") {
    val logs_category = DataLoader.logsWithCategory(spark, path_categories, logs)
    val most_popular_testing = DataLoader.makeMostPopular(logs_category).
      withColumn("count", when(col("count").isNotNull, col("count")).otherwise(lit(null)))
    val most_popular_expecting = spark.table(s"$db_test.popular_category_benchmark")
    assertSmallDatasetEquality(most_popular_testing, most_popular_expecting, orderedComparison = false)
  }

}
