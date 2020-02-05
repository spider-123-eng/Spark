package com.spark2.window.functions
import org.apache.spark.sql.SparkSession
import java.sql.Date
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CPUTidSiteRollup extends App {

  val spark = SparkSession.builder().appName("SwitchCPU").master("local[1]").getOrCreate()
  import spark.implicits._

  val input_switch_cpu = "input/iap_sw_cpu_mem_stats_rollup"

  val dfIntermed = spark.read.parquet(input_switch_cpu)
  dfIntermed.show(3, false)
  dfIntermed.printSchema()

  var dfRollup = dfIntermed
    .groupBy("tid")
    .agg(countDistinct("serial_number").cast(IntegerType).as("num_switches_impacted"))
    .withColumn("data_type", lit("iap_insight"))

  dfRollup.show(5, false)

  val dfMpdelRollUp = dfIntermed
    .groupBy("tid", "model")
    .agg(countDistinct("serial_number").alias("num_switches_impacted"))
    .withColumn("model_switch_count", struct(
      $"model".as("model"),
      $"num_switches_impacted".as("count")))
    .groupBy("tid")
    .agg(collect_list("model_switch_count").alias("model_switch_count_list"))

  dfMpdelRollUp.show(5, false)

  val dfFirmwareRollup = dfIntermed
    .groupBy("tid", "firmware")
    .agg(countDistinct("serial_number").alias("num_switches_impacted"))
    .withColumn("firmware_switch_count", struct(
      $"firmware".as("firmware"),
      $"num_switches_impacted".as("count")))
    .groupBy("tid")
    .agg(collect_list("firmware_switch_count").alias("firmware_switch_count_list"))

  dfRollup = dfRollup.join(
    dfMpdelRollUp,
    Seq("tid"), "left_outer")
    .join(
      dfFirmwareRollup,
      Seq("tid"), "left_outer")
    .withColumn("timeline_metric", $"num_switches_impacted".cast(FloatType))

  dfRollup.show(5, false)
}
