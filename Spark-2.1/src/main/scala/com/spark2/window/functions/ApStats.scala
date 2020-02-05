package com.spark2.window.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ApStats extends App {

  val spark = SparkSession.builder().appName("ApStats").master("local[1]").getOrCreate()

  import spark.implicits._

  val input_switch_cpu = "input/iap_ap_info_records/date_month=2020-01/date_hour=2020-01-24-11/*"

  val df = spark.read.parquet(input_switch_cpu)

  df.printSchema()

  val stats = df.select($"tid", $"ap_name", $"ap_mac".as("apmac"), $"cpu_usage", $"memory_total", $"memory_free", $"ts")
    .withColumn(
      "memory_usage",
      lit(((col("memory_total") - col("memory_free")) / col("memory_total")) * 100))
    .withColumn("temp_ts", col("ts").divide(1000).cast("timestamp"))
    .select("tid", "ap_name", "apmac", "cpu_usage", "memory_usage", "temp_ts")
    .withColumn("cpu_timeseries", struct(
      $"temp_ts".cast("long").as("ts"),
      $"cpu_usage".cast("float").as("avg"),
      $"cpu_usage".cast("float").as("max")))
    .withColumn("memory_timeseries", struct(
      $"temp_ts".cast("long").as("ts"),
      $"memory_usage".cast("float").as("avg"),
      $"memory_usage".cast("float").as("max")))
    .groupBy(col("tid"), col("apmac"),
      window(col("temp_ts"), "1 hour").alias("ts")).
      agg(
        avg("cpu_usage").as("cl_ap_system_stats_cpu_util"),
        avg("memory_usage").as("cl_ap_system_stats_mem_util"),
        collect_list($"cpu_timeseries").as("cpu_timeseries"),
        collect_list($"memory_timeseries").as("memory_timeseries"))
    .withColumn("ts_hr", hour($"ts.start"))


  stats.printSchema()
  stats.show(5, false)
}
