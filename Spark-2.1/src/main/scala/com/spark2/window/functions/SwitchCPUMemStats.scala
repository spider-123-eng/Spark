package com.spark2.window.functions
import java.sql.Date
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import java.time.{ Instant, ZoneId, ZonedDateTime }
import java.time.format.DateTimeFormatter

object SwitchCPUMemStats extends App {

  val spark = SparkSession.builder().appName("SwitchCPU").master("local[1]").getOrCreate()

  import spark.implicits._

  val input_switch_cpu = "/Users/revanth/temp/sw_hp_system_cpu_stats_records/date_month={2020}-{01}/date_hour={2020}-{01}-{13}-{04,05}"
  val input_switch_mem = "/Users/revanth/temp/sw_hp_system_memory_stats_records/date_month={2020}-{01}/date_hour={2020}-{01}-{13}-{04,05}"
  val input_switch_sys = "/Users/revanth/temp/sw_hp_system_info_stats_records/date_month={2020}-{01}/date_hour={2020}-{01}-{13}-{04,05}"

  import org.apache.spark.sql.expressions.UserDefinedFunction
  import org.apache.spark.sql.functions.udf

  val parseEpoc: UserDefinedFunction = udf((millis: Long) => {
    val instant = Instant.ofEpochMilli(millis)
    val zonedDateTimeUtc = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
    val dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
    val zonedDateTimeUtcString = dateTimeFormatter.format(zonedDateTimeUtc)
    val date = zonedDateTimeUtcString.replace("T", " ")
    date
  })
  val df_cpu_statsDF = spark.read.parquet(input_switch_cpu)

  val df_mem_statsDF = spark.read.parquet(input_switch_mem)
  val df_sys_statsDF = spark.read.parquet(input_switch_sys)

  val filtInsightDF = df_cpu_statsDF
    .select($"tid", $"serial_number", $"current_cpu_usage_percentage", $"ts")
    .withColumn("ts_slot", ((col("ts") / 300000)).cast("integer"))
    .withColumn("temp_ts", parseEpoc(col("ts")))
    .alias("cpu_stats_df")

  filtInsightDF.show(5, false)

  val tsColumn = "temp_ts"
  val startTime = "2020-01-13 04:30"
  val endTime = "2020-01-13 05:30"

  val cpuDF = filtInsightDF.filter("" + tsColumn + " >= cast('" + startTime + "' as timestamp) and " + tsColumn + " < cast('" + endTime + "' as timestamp)")

  println("input_switch_cpu couunt records : " + cpuDF.count())

  val memoryDF = df_mem_statsDF
    .select($"tid", $"serial_number", $"current_free_memory_in_bytes", $"ts")
    .withColumn("ts_slot", ((col("ts") / 300000)).cast("integer")).alias("mem_stats_df")

  val sysInfoDF = df_sys_statsDF
    .select($"tid", $"serial_number", $"total_memory_in_bytes", $"product_model".cast("string"), $"firmware_version", $"ts")
    .withColumn("ts_slot", ((col("ts") / 300000)).cast("integer")).alias("sysInfo_stats_df")

  val cpuMemJoinDF = cpuDF.join(memoryDF, Seq("serial_number", "ts_slot"), "left_outer")

  val cpuMemSysInfoJoinDF = cpuMemJoinDF.join(sysInfoDF, Seq("serial_number", "ts_slot"), "left_outer")

  cpuMemSysInfoJoinDF.printSchema

  val memUsedDF = cpuMemSysInfoJoinDF.select($"serial_number", $"ts_slot", $"current_free_memory_in_bytes", $"total_memory_in_bytes",
    $"current_cpu_usage_percentage", $"cpu_stats_df.tid".as("tid"), $"cpu_stats_df.ts".as("ts"), $"product_model", $"firmware_version")
    .withColumn("temp_ts", col("ts").divide(1000).cast("timestamp"))
    .withColumn(
      "memory_usage",
      lit(((col("total_memory_in_bytes") - col("current_free_memory_in_bytes")) / col("total_memory_in_bytes")) * 100))
    .withColumnRenamed("current_cpu_usage_percentage", "cpu_usage")

  memUsedDF.printSchema

  val stats = memUsedDF
    .withColumn("cpu_timeseries", struct(
      $"temp_ts".cast("long").as("ts"),
      $"cpu_usage".cast("float").as("avg"),
      $"cpu_usage".cast("float").as("max")))
    .withColumn("memory_timeseries", struct(
      $"temp_ts".cast("long").as("ts"),
      $"memory_usage".cast("float").as("avg"),
      $"memory_usage".cast("float").as("max")))
    .groupBy(col("serial_number"), col("tid"), col("product_model"), col("firmware_version"),
      window(col("temp_ts"), "1 hour").alias("ts"))
    .agg(
      avg("cpu_usage").as("cpu_util"),
      avg("memory_usage").as("mem_util"),
      collect_list($"cpu_timeseries").as("cpu_timeseries"),
      collect_list($"memory_timeseries").as("memory_timeseries"))
    .withColumnRenamed("product_model", "model")
    .withColumnRenamed("firmware_version", "firmware")
    .withColumn("ts_hr", hour($"ts.start"))
    .withColumn("site", $"tid")

  stats.printSchema()
  stats.filter(s"cpu_util > 30").select("tid", "serial_number", "cpu_util", "cpu_timeseries")
    .sort(col("cpu_util")).show(5, false)

  //    val siteDF = Seq(
  //      ("CN80FP52RB", "Aruba_BLR"),
  //      ("CN83JYL1KN", "Aruba_US"),
  //      ("CN8AJYK0YS", "HPE_BLR"),
  //      ("SG7BJQL6QW", "HPE_US"),
  //      ("CN84FP90GL", "Aruba_CH")).toDF("serial_number", "site")
  //
  //    val sitesDF = siteDF.select("serial_number", "site").distinct()
  //
  //    val resultDF = stats.join(sitesDF, Seq("serial_number"), "left_outer").na.fill("Aruba_BLR",Array("site"))
  //    stats.show(10, false)

  //println(stats.filter($"serial_number" === "CN41FP415K").count())
  //stats.filter($"serial_number" === "CN41FP415K").show(false)

  //  println(stats.count())
  //  println(stats.filter($"serial_number" === "SG83FLXZG2").count())
  //val getConcatenated = udf((first: String, second: String) => { first + "_" + second })

}
