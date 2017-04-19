package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object MovingAverage {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark-MovingAverage").setMaster("local[1]") 
        //.set("spark.sql.warehouse.dir", "file:///E:/MyStuff/HadoopProj/Scala/WorkSpace/Spark/spark-warehouse")
    )
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val customers = sc.parallelize(List(
      ("Alice", "2016-05-01", 50.00),
      ("Alice", "2016-05-03", 45.00),
      ("Alice", "2016-05-04", 55.00),
      ("Bob", "2016-05-01", 25.00),
      ("Bob", "2016-05-04", 29.00),
      ("Bob", "2016-05-06", 27.00))).
      toDF("name", "date", "amountSpent")

    // Create a window spec.
    val wSpec1 = Window.partitionBy("name").orderBy("date").rowsBetween(-1, 1)

    // Calculate the moving average
    customers.withColumn("movingAvg",
      avg(customers("amountSpent")).over(wSpec1)).show()

    val wSpec2 = Window.partitionBy("name").orderBy("date").rowsBetween(Long.MinValue, 0)

    // Create a new column which calculates the sum over the defined window frame.
    customers.withColumn("cumSum",
      sum(customers("amountSpent")).over(wSpec2)).show()

    // Window spec. No need to specify a frame in this case.
    val wSpec3 = Window.partitionBy("name").orderBy("date")

    // Use the lag function to look backwards by one row.
    customers.withColumn("prevAmountSpent",
      lag(customers("amountSpent"), 1).over(wSpec3)).show()

    customers.withColumn("rank", rank().over(wSpec3)).show()
  }
}