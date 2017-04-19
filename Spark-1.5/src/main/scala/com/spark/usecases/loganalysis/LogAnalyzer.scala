package com.spark.usecases.loganalysis
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.SparkContext._
import com.spark.util._
object LogAnalyzer {

  object SecondValueOrdering extends Ordering[(String, Int)] {
    def compare(a: (String, Int), b: (String, Int)) = {
      a._2 compare b._2
    }
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Log Analysis").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val accessLogs = sc.textFile(Utills.DATA_PATH + "log.txt")
      .map(ApacheAccessLog.parseLogLine).cache()

    // Any IPAddress that has accessed the server more than 2 times.
    val ipAddresses = accessLogs
      .map(log => (log.ipAddress, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 4)
      .map(_._1)
      .take(10)
    println(s"""IPAddresses > 2 times: ${ipAddresses.mkString("[", ",", "]")}""")

    // Finding top 5 hits.
    val ipAddressesTop5 = accessLogs
      .map(log => (log.ipAddress, 1))
      .reduceByKey(_ + _)
      .top(5)(SecondValueOrdering)

    println(s"""Top 5 hits : ${ipAddressesTop5.mkString("[", ",", "]")}""")

    // Top Endpoints.
    val topEndpoints = accessLogs
      .map(log => (log.endpoint, 1))
      .reduceByKey(_ + _)
      .top(10)(SecondValueOrdering)
    println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")

    // Calculate statistics based on the content size.
    val contentSizes = accessLogs.map(log => log.contentSize).cache()
    println("Content Size Avg: %s, Min: %s, Max: %s".format(
      contentSizes.reduce(_ + _) / contentSizes.count,
      contentSizes.min,
      contentSizes.max))

    // Compute Response Code to Count.
    val responseCodeToCount = accessLogs
      .map(log => (log.responseCode, 1))
      .reduceByKey(_ + _)
      .take(100)
    println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")
  }
}