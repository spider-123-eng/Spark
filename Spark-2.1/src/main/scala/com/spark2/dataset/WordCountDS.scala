package com.spark2.dataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountDS {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("WordCountDS Application").master("local[1]").getOrCreate()

    import session.implicits._
    val sc = session.sparkContext
    val sqlContext = new SQLContext(sc)

    //With Spark DataSets API
    //Since the Dataset version of word count can take advantage of the built-in aggregate count,
    // this computation can not only be expressed with less code, but it will also execute significantly faster.

    val ds = sqlContext.read.text("input/README.md").as[String]
    val result = ds
      .flatMap(_.split(" ")) // Split on whitespace
      .filter(_ != "") // Filter empty words
      .toDF() // Convert to DataFrame to perform aggregation / sorting
      .groupBy($"value") // Count number of occurences of each word
      .agg(count("*") as "numOccurances")
      .orderBy($"numOccurances" desc) // Show most common words first

    result.foreach { x => println(x) }

  }
}