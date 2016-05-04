package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Calendar
import org.apache.spark.sql.SQLContext

object WordCount {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local[1]"))
    
    val today = Calendar.getInstance().getTime()

    val threshold = 2

    // split each document into words
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    // filter out words with less than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)

    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    //wordCounts.saveAsTextFile(args(1))
    println("---------------------------------------------------")
     println(charCounts.collect().mkString(", "))

  }
}