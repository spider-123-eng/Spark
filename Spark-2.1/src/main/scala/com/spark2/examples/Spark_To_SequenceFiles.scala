package com.spark2.examples

import org.apache.spark.sql.SparkSession

object Spark_To_SequenceFiles {
  case class Purchase(customer_id: Int, purchase_id: Int, date: String, time: String, tz: String, amount: Double)

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("Spark_To_SequenceFiles").master("local[1]").getOrCreate()

    import session.implicits._
    val purchaseDF = List(
      Purchase(121, 234, "2017-04-19", "20:50", "UTC", 500.99),
      Purchase(122, 247, "2017-05-20", "15:30", "PST", 300.22),
      Purchase(123, 254, "2016-03-09", "00:50", "EST", 122.19),
      Purchase(124, 234, "2016-02-14", "20:50", "UTC", 500.99),
      Purchase(125, 247, "2015-01-13", "15:30", "PST", 300.22),
      Purchase(126, 254, "2015-05-16", "00:50", "EST", 122.19),
      Purchase(127, 250, "2016-09-17", "15:30", "PST", 300.22),
      Purchase(128, 251, "2018-08-15", "00:50", "EST", 122.19),
      Purchase(129, 299, "2019-02-19", "07:30", "UTC", 524.37)).toDF()

    import org.apache.spark.rdd.RDD
    import org.apache.spark.sql.Row

    val purchaseRDD: RDD[(Int, String)] = purchaseDF.rdd.map {
      case r: Row => (r.getAs[Int](0), r.getAs[String](2))
    }
    purchaseRDD.saveAsSequenceFile("output/rdd_to_seq")

    //Loading sequenceFiles into an RDD in Spark

    val data: RDD[(Int, String)] = session.sparkContext.sequenceFile("output/rdd_to_seq")

    data.foreach(f => println(f))

  }
}