package com.spark2.dataframes

import org.apache.spark.sql.SparkSession
object PartitionBy {

  case class Purchase(customer_id: Int, purchase_id: Int, date: String, time: String, tz: String, amount: Double)

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("Spark-PartitionBy").master("local[1]").getOrCreate()

    import session.implicits._
    val purchaseDF = List(
      Purchase(121, 234, "2017-04-19", "20:50", "UTC", 500.99),
      Purchase(122, 247, "2017-04-19", "15:30", "PST", 300.22),
      Purchase(123, 254, "2017-04-19", "00:50", "EST", 122.19),
      Purchase(124, 234, "2017-04-19", "20:50", "UTC", 500.99),
      Purchase(125, 247, "2017-04-19", "15:30", "PST", 300.22),
      Purchase(126, 254, "2017-04-19", "00:50", "EST", 122.19),
      Purchase(125, 250, "2017-04-19", "15:30", "PST", 300.22),
      Purchase(126, 251, "2017-04-19", "00:50", "EST", 122.19),
      Purchase(127, 299, "2017-04-19", "07:30", "UTC", 524.37)).toDF()

    purchaseDF.coalesce(1).write.parquet("input/parqOut1")

    val df = session.read.parquet("input/parqOut1")

    val duplicated = df.withColumn("_cust_id", $"customer_id")

    duplicated.coalesce(1).write.partitionBy("_cust_id").csv("input/csv/")
  }
}