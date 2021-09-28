package com.rostel

import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.BufferedOutputStream
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{from_unixtime, monotonically_increasing_id, row_number, to_date}

object DataLoader extends SparkSessionWrapper {

  import spark.implicits._

  //  Define constants
  val dt: String = "2019-12-11"
  val path: String = "hdfs:/user/sergey/clickstream"
  val path_logs: String = path + "/" + dt.replaceAll("-", "") + "/*.tsv"
  val path_metrics: String = path + "/" + dt.replaceAll("-", "") + "/" + "metrics.txt"
  val path_categories: String = "hdfs:/user/sergey/categories/categories.tsv"
  val path_mrf: String = "hdfs:/user/sergey/regions/regions.tsv"
  val db_out: String = "rostel"
  val db_test: String = "rostel_test"

  def main(args: Array[String]): Unit = {

    LogManager.getLogger("org").setLevel(Level.WARN)
    spark.sql(s"use $db_out")

    //load raw data
    val t0 = System.currentTimeMillis()
    val raw_logs = loadRawLogs(spark, path_logs)
    raw_logs.show(5, false)
    println(s"Loaded raw data in: ${(System.currentTimeMillis() - t0) / 1000.0} s")

    //parse raw logs
    val t1 = System.currentTimeMillis()
    val logs = parseRawLogs(raw_logs)
    logs.show(5, false)
    println(s"Parsed raw data in: ${(System.currentTimeMillis() - t1) / 1000.0} s")

    //enrich parsed logs with mrf codes and write to Hive
    val t2 = System.currentTimeMillis()
    writeLogsWithMRF(spark, path_mrf, logs)
    println(s"Wrote logs enriched by mrf codes to Hive database in ${(System.currentTimeMillis() - t2) / 1000.0} s")

    //make first_session table and write it to Hive
    val t3 = System.currentTimeMillis()
    val first_session = makeFirstSession(logs: DataFrame)
    first_session.show(5, false)
    writeFirstSession(first_session)
    println(s"Wrote first session for a user to Hive database in ${(System.currentTimeMillis() - t3) / 1000.0} s")

    //most popular category per user
    val t4 = System.currentTimeMillis()
    val logs_category = logsWithCategory(spark, path_categories, logs)
    val popular_category = makeMostPopular(logs_category)
    popular_category.show(5, false)
    writePopularCategory(popular_category)
    println(s"Wrote most popular category for a user to Hive database in ${(System.currentTimeMillis() - t4) / 1000.0} s")

    //METRICS
    val t5 = System.currentTimeMillis()
    writeMetrics(raw_logs, logs_category, logs)
    println(s"Calculated metrics in ${(System.currentTimeMillis() - t5) / 1000.0} s")
    println(s"Total time spent: ${(System.currentTimeMillis() - t0) / 1000.0} s")
  }

  def loadRawLogs(spark: SparkSession, path_logs: String): DataFrame = {
    val schema_logs_raw = "timestamp string, url string, login string, region_code string"
    spark.
      read.
      options(Map("header" -> "true", "sep" -> "\t")).
      schema(schema_logs_raw).
      csv(path_logs).
      withColumn("id", monotonically_increasing_id).
      withColumn("valid_today", to_date(from_unixtime('timestamp)) === dt).
      withColumn("valid_not_today", to_date(from_unixtime('timestamp)) =!= dt).
      withColumn("not_timestamp", from_unixtime('timestamp).isNull).cache()
  }

  def parseRawLogs(raw_logs: DataFrame): DataFrame = {
    raw_logs.
      filter('valid_today || 'not_timestamp).
      selectExpr(
        "cast(timestamp as long) timestamp",
        "url",
        "login",
        "cast(region_code as integer) region_code",
        "id"
      )
  }

  def writeLogsWithMRF(spark: SparkSession, path_regions: String, logs: DataFrame): Unit = {
    val schema_regions = "regione_code integer, mrf string"
    val mrf = spark.
      read.
      options(Map("header" -> "true", "sep" -> "\t")).
      schema(schema_regions).
      csv(path_regions)
    val region_mrf = logs.
      join(mrf.hint("broadcast"), logs("region_code") === mrf("regione_code"), "left").
      drop("regione_code")
    region_mrf.show(5, false)
    // write to Hive
    region_mrf.
      write.
      mode("overwrite").
      saveAsTable("logs_mrf")
  }

  def makeFirstSession(logs: DataFrame): DataFrame = {
    val w1 = Window.partitionBy('login).orderBy('timestamp.desc)
    logs.
      withColumn("rn", row_number.over(w1)).where('rn === 1).
      select('login, 'timestamp)
  }

  def writeFirstSession(first_session: DataFrame): Unit = {
    first_session.
      coalesce(2).
      write.
      mode("overwrite").
      saveAsTable("first_session")
  }

  def logsWithCategory(spark: SparkSession, path_categories: String, logs: DataFrame): DataFrame = {
    val schema_categories = "url string, category string"
    val categories = spark.
      read.
      options(Map("header" -> "true", "sep" -> "\t")).
      schema(schema_categories).
      csv(path_categories)
    val logs_host = logs.
      selectExpr("login", "from_unixtime(timestamp) as date", "parse_url(url, 'HOST') as host", "url", "id")
    val categories_host = categories.
      selectExpr("parse_url(url, 'HOST') as host", "category")
    logs_host.
      join(categories_host.hint("broadcast"), Seq("host"), "left")
  }

  def makeMostPopular(logs_category: DataFrame): DataFrame = {
    val w2 = Window.partitionBy('login).orderBy('count.desc)
    logs_category.
      groupBy('login, 'category).count().
      withColumn("rn", row_number.over(w2)).
      filter('rn === 1).
      drop('rn)
  }

  def writePopularCategory(popular_category: DataFrame): Unit = {
    popular_category.
      coalesce(2).
      write.
      mode("overwrite").
      saveAsTable("popular_category")
  }

  def writeMetrics(raw_logs: DataFrame, logs_category: DataFrame, logs: DataFrame): Unit = {

    val num_total = raw_logs.count
    println(s"Total number of records: $num_total")

    val num_timestamp_today = raw_logs.
      filter('valid_today).
      count
    println(s"Number of todays's valid timestamps: $num_timestamp_today")

    val id_timestamp_not_today = raw_logs.
      filter('valid_not_today).
      select("id").as[Long].
      collect
    val num_timestamps_not_today = id_timestamp_not_today.length
    println(s"Number of valid not today's timestamps: $num_timestamps_not_today")

    val id_not_timestamp = raw_logs.
      filter('not_timestamp).
      select("id").
      as[Long].collect
    val num_not_timestamps = id_not_timestamp.length
    println(s"Number of not valid timestamps: $num_not_timestamps")

    val id_empty_login = raw_logs.
      filter('login.isNull).
      select("id").
      as[Long].collect
    val num_empty_login = id_empty_login.length
    println(s"Number of empty logins $num_empty_login")


    // "Bad" URL: the host is not in the lookup table
    val id_bad_url = logs_category.
      filter('category.isNull).
      select("id").
      as[Long].collect
    val num_bad_url = id_bad_url.length
    println(s"Number of bad URLs (the host is not in the lookup table) : $num_bad_url")

    val id_bad_region = logs.
      filter('region_code.isNull || 'region_code > 100 || 'region_code < 0).
      select("id").
      as[Long].
      collect
    val num_bad_region = id_bad_region.length
    println(s"Number of bad regions: $num_bad_region")

    println(s"Saving metrics to $path_metrics")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val output = fs.create(new Path(path_metrics))
    val os = new BufferedOutputStream(output)
    os.write(
      s"""
      Total number of records is : $num_total
Number of valid today timestamps : $num_timestamp_today
  Number of not valid timestamps : $num_not_timestamps
          Number of empty logins : $num_empty_login
              Number of bad URLs : $num_bad_url
       Number of bad region_code : $num_bad_region
""".getBytes("UTF-8"))
    os.close()

    // Write bad logs to Hive table for visual inspection
    val bad_logs_id = id_timestamp_not_today ++ id_not_timestamp ++ id_empty_login ++ id_bad_url ++ id_bad_region
    val bad_logs = raw_logs.
      filter('id.isin(bad_logs_id: _*)).
      select("timestamp", "url", "login", "region_code", "id")
    bad_logs.
      write.
      mode("overwrite").
      saveAsTable("bad_data")
    println("Showing sample of bad data:")
    bad_logs.show(5, false)
  }
}
