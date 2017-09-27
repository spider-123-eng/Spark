package com.spark2.cassandra

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import scala.annotation.migration
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
/**
 * @author revanthreddy
 * This Object is used to backup the previous day table data from Cassandra.
 */
object ExportCassandraData {

  protected val logger = LoggerFactory.getLogger(getClass)

  def getDateMonth(): String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.format(cal.getTime())
    date.substring(0, date.length() - 3)
  }
  def getPrevDate(day: Int): String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -day)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(cal.getTime()) + " 00:00"
  }

  def getExcutionTime(): Date = {
    val cal = Calendar.getInstance()
    cal.getTime()
  }
  val spark = SparkSession.builder().appName("Export-Cassandra-Data-Job")
    .getOrCreate()

  /**
   * This method is used to export previous days data for the given cassandra table
   *
   */
  def exportCassandraData(table: String, cluster: String, outputPath: String): Unit = {
    try {
      import spark.implicits._

      val insightDF = (spark.sqlContext
        .read.format("org.apache.spark.sql.cassandra")
        .options(Map(
          "cluster" -> cluster,
          "table" -> table,
          "keyspace" -> "database"))
        .load)

      //filter only previous day data
      val filtInsightDF = insightDF.filter("timestamp > cast('" + getPrevDate(1) + "' as timestamp) and timestamp <= cast('" + getPrevDate(0) + "' as timestamp)")
      val df_tid = filtInsightDF.select("tid", "timestamp").distinct
      val tid_ts = df_tid.as[(Int, String)].rdd.collect.toMap // Mapping of tid and timestamp

      tid_ts.keys.foreach { id =>
        var date_hour_list = tid_ts(id).split(" ")
        var date = date_hour_list(0)
        var hour_min_sec = date_hour_list(1).split(":")
        var hour = hour_min_sec(0)

        logger.info(getExcutionTime + ": Started Working on tid = " + id + " with UTC offset = " + date_hour_list(1))

        var ts1 = tid_ts(id)
        val filterInsightDb = filtInsightDF.filter($"tid" === id)
        val readCount = filterInsightDb.count
        if (readCount != 0) {

          logger.info(getExcutionTime + " Writing to : " + outputPath + "tid="
            + id + "/date_month=" + getDateMonth + "/date_hour=" + date + "-" + hour)

          filterInsightDb.write.
            parquet(outputPath + "/tid=" + id + "/date_month=" + getDateMonth + "/date_hour=" + date + "-" + hour)

          logger.info(getExcutionTime + ": Ended Writing to tid = " + id + " with UTC offset = " + date_hour_list(1))

          val writeCount = spark.read.parquet(outputPath + "/tid=" + id + "/date_month=" + getDateMonth + "/date_hour=" + date + "-" + hour).count()

          if (readCount == writeCount) {
            logger.info("Both the count matches")
          } else {
            logger.info("Record Count not mathed")
          }
        }
        logger.info(getExcutionTime + ": Ended Working on tid = " + id + " with UTC offset = " + date_hour_list(1))
      }
    } catch {
      case e: Exception =>
        logger.error("Error in ExportCassandraData.exportCassandraData() " + e.getMessage)
        e.printStackTrace()
    }

  }
  def main(arg: Array[String]): Unit = {
    try {
      val CASSANDRA_HOST = arg(0)
      val CLUSTER_NAME = arg(1)
      val CASSANDRA_USERNAME = arg(1)
      val CASSANDRA_PASSWORD = arg(2)
      val outPut_Path = arg(3)
      val table_name = arg(4)

      spark.conf.set(CLUSTER_NAME + "/spark.cassandra.connection.host", CASSANDRA_HOST)
      spark.conf.set(CLUSTER_NAME + "/spark.cassandra.auth.username", CASSANDRA_USERNAME)
      spark.conf.set(CLUSTER_NAME + "/spark.cassandra.auth.password", CASSANDRA_PASSWORD)

      spark.sqlContext.setConf(CLUSTER_NAME + "/spark.input.consistency.level", "QUORUM")
      spark.sqlContext.setConf(CLUSTER_NAME + "/spark.cassandra.connection.timeout_ms", "10000")
      spark.sqlContext.setConf(CLUSTER_NAME + "/spark.cassandra.read.timeout_ms", "3600000")
      spark.sqlContext.setConf(CLUSTER_NAME + "/spark.cassandra.input.split.size_in_mb", "512")

      //Exporting the Cassandra 'insight' table data
      exportCassandraData(table_name, CLUSTER_NAME, outPut_Path)

    } catch {
      case e: Exception =>
        logger.error("Error in ExportCassandraData.main() " + e.getMessage)
        e.printStackTrace()
    }
  }
}
