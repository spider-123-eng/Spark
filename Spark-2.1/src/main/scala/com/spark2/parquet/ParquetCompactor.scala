package com.spark2.parquet

import org.apache.spark.sql.SparkSession

object ParquetCompactor {
  case class Purchase(customer_id: Int, purchase_id: Int, date: String, time: String, tz: String, amount: Double)

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("ParquetCompactor").master("local[1]").getOrCreate()

    import session.implicits._
    val purchaseDF = List(
      Purchase(121, 234, "2017-04-19", "20:50", "UTC", 500.99),
      Purchase(122, 247, "2017-04-19", "15:30", "PST", 300.22),
      Purchase(185, 254, "2017-04-19", "00:50", "EST", 122.19),
      Purchase(186, 299, "2017-04-19", "07:30", "UTC", 524.37)).toDF()

    purchaseDF.write.parquet("input/parqOut")

    val df = session.read.parquet("input/parqOut")

    df.show()
    print("count before dropping :" + df.count())

    //dropping the duplicate rows based on customer_id
    val dropedDF = df.dropDuplicates("customer_id")

    println("count after dropping :" + dropedDF.count())
    dropedDF.show()

  }
}