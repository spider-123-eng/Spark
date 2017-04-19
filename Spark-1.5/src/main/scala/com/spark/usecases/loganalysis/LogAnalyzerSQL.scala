package com.spark.usecases.loganalysis
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkContext, SparkConf }
import com.spark.util._
object LogAnalyzerSQL {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Log Analyzer SQL").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val accessLogs = sc.textFile(Utills.DATA_PATH + "log.txt").map(ApacheAccessLog.parseLogLine).toDF()
    accessLogs.registerTempTable("Logs")
    sqlContext.cacheTable("Logs");

    // Calculate statistics based on the content size.
    val contentSizeStats = sqlContext
      .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM Logs")
      .first()
    println("Content Size Avg: %s, Min: %s, Max: %s".format(
      contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
      contentSizeStats(2),
      contentSizeStats(3)))

    // Compute Response Code to Count.
    val responseCodeToCount = sqlContext
      .sql("SELECT responseCode, COUNT(*) FROM Logs GROUP BY responseCode LIMIT 1000")
      .map(row => (row.getInt(0), row.getLong(1)))
      .collect()
    println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

    // Any IPAddress that has accessed the server more than 10 times.
    val ipAddresses = sqlContext
      .sql("SELECT ipAddress, COUNT(*) AS total FROM Logs GROUP BY ipAddress HAVING total > 10 LIMIT 1000")
      .map(row => row.getString(0))
      .collect()
    println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

    val topEndpoints = sqlContext
      .sql("SELECT endpoint, COUNT(*) AS total FROM Logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()
    println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")

    sc.stop()
  }
}