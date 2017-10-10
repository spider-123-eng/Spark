package com.spark2.dataframes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object PartitionBy_withUDF {

  case class Purchase(customer_id: Int, purchase_id: Int, date: String, time: String, tz: String, amount: Double)

  val setDateMonth: (String) => String = (timeStamp: String) => {
    var date_hour_list = timeStamp.split(" ")
    var date = date_hour_list(0)
    var month = date.split("-")
    month(0) + "-" + month(1)
  }
  val setDateHour: (String) => String = (timeStamp: String) => {
    var date_hour_list = timeStamp.split(" ")
    var date = date_hour_list(0)
    var month = date.split("-")
    month(0) + "-" + month(1)
    var hour_min_sec = date_hour_list(1).split(":")
    date + "-" + hour_min_sec(0)
  }
  val getDateMonth = udf(setDateMonth)
  val getDateHour = udf(setDateHour)

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("Spark-PartitionBy").master("local[1]").getOrCreate()

    import session.implicits._
    val purchaseDF = List(
      Purchase(121, 234, "2017-09-26 05:00:00.0", "20:50", "UTC", 500.99),
      Purchase(122, 247, "2017-07-26 05:00:00.0", "15:30", "PST", 300.22),
      Purchase(123, 254, "2017-09-26 05:00:00.0", "00:50", "EST", 122.19),
      Purchase(124, 234, "2017-09-26 04:00:00.0", "20:50", "UTC", 500.99),
      Purchase(125, 247, "2017-08-26 05:00:00.0", "15:30", "PST", 300.22),
      Purchase(126, 254, "2017-08-26 05:00:00.0", "00:50", "EST", 122.19),
      Purchase(125, 250, "2017-08-26 05:00:00.0", "15:30", "PST", 300.22),
      Purchase(121, 251, "2017-07-26 07:00:00.0", "00:50", "EST", 122.19),
      Purchase(127, 299, "2017-07-26 05:00:00.0", "07:30", "UTC", 524.37)).toDF()

    purchaseDF.coalesce(1).write.parquet("input/parqOut1")

    val df = session.read.parquet("input/parqOut1")

    df.printSchema()

    val finalDF = df.withColumn("date_month", getDateMonth(df.col("date"))).withColumn("date_hour", getDateHour(df.col("date")))

    finalDF.coalesce(1).write.mode(SaveMode.Overwrite).partitionBy("date_month", "date_hour", "customer_id").csv("input/csv/")
  }
}