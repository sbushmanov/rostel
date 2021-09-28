package com.rostel

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark = SparkSession.builder()
    .master("local[*]")
    .appName("loader123")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("hive.metastore.uris", "thrift://localhost:9083")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .enableHiveSupport()
    .getOrCreate()
}
