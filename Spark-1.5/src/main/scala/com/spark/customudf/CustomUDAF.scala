package com.spark.customudf

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.functions._

object CustomUDAF {
  val sparkConf = new SparkConf().setAppName("Spark-CustomUDAF").setMaster("local[1]")//.set("spark.sql.warehouse.dir", "file:///D:/Spark-WorkSpace/Spark-Windows/spark-warehouse")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  def main(args: Array[String]) {
    // define UDAF
    val customMean = new CustomMean()

    // create test dataset
    val data = (1 to 100).map { x: Int =>
      x match {
        case t if t <= 50 => Row("A", t.toDouble)
        case t            => Row("B", t.toDouble)
      }
    }

    // create schema of the test dataset
    val schema = StructType(Array(
      StructField("key", StringType),
      StructField("value", DoubleType)))

    // construct data frame
    val rdd = sc.parallelize(data)
    val df = sqlContext.createDataFrame(rdd, schema)

    // Calculate average value for each group
    df.groupBy("key").agg(
      customMean(df.col("value")).as("custom_mean"),
      avg("value").as("avg")).show()
  }
}