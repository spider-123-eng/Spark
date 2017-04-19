package com.spark2.examples

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

    //purchaseDF.write.parquet("input/parqOut1")

    //    val df = session.read.parquet("input/parqOut")
    //    
    //    df.show()
    //    print("count before :" + df.count())
    //    val dropedDF = df.dropDuplicates("customer_id")
    //    println("count after :" + dropedDF.count())
    //    dropedDF.show()

    val df = session.read.parquet("/Users/revanthreddy/Desktop/date_month=2017-04")
    df.printSchema()
    println("count before :" + df.count())
    //df.write.parquet("input/parqOut1")

    val resdf = session.read.parquet("input/parqOut1")
    println("count after :" + resdf.count())
  }
}