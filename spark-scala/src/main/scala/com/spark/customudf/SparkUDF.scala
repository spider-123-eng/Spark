package com.spark.customudf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkUDF {
  case class Purchase(customer_id: Int, purchase_id: Int, date: String, time: String, tz: String, amount: Double)

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark-custom-UDF").setMaster("local[1]"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val x = sc.parallelize(Array(
      Purchase(123, 234, "2007-12-12", "20:50", "UTC", 500.99),
      Purchase(123, 247, "2007-12-12", "15:30", "PST", 300.22),
      Purchase(189, 254, "2007-12-13", "00:50", "EST", 122.19),
      Purchase(187, 299, "2007-12-12", "07:30", "UTC", 524.37)))

    val df = sqlContext.createDataFrame(x)
    df.registerTempTable("df")

    def makeDT(date: String, time: String, tz: String) = s"$date $time $tz"
    sqlContext.udf.register("makeDt", makeDT(_: String, _: String, _: String))

    // Now we can use our function directly in SparkSQL.
    val res = sqlContext.sql("SELECT amount, makeDt(date, time, tz) from df").take(2)
    res.foreach { x => print(x) }

    // but not outsideit fails
    // df.select($"customer_id", makeDt($"date", $"time", $"tz"), $"amount").take(2)

    //You can see above that we can use it within SQL but not outside of it. 
    //To do that we're going to have to create a different UDF using spark.sql.function.udf

    import org.apache.spark.sql.functions.udf
    val makeDt = udf(makeDT(_: String, _: String, _: String))
    // now this works
    df.select($"customer_id", makeDt($"date", $"time", $"tz"), $"amount").take(2).foreach { x => print(x) }

    //    In Spark version 1.5, functions to create date times were introduced. 
    //    Now we can leave our function the same however we're just going to create a format and wrap our MakeDT 
    //    function in the unix_timestampfunction call, we can do this both in and out of SparkSQL!

    import org.apache.spark.sql.functions.unix_timestamp

    val fmt = "yyyy-MM-dd hh:mm z"
    df.select($"customer_id", unix_timestamp(makeDt($"date", $"time", $"tz"), fmt), $"amount").take(2).foreach { x => print(x) }

    sqlContext.sql(s"SELECT customer_id, unix_timestamp(makeDt(date, time, tz), '$fmt'), amount FROM df").take(2).foreach { x => print(x) }
  }
}