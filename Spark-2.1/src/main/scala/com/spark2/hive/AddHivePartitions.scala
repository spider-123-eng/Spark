package com.spark2.hive

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory

object AddHivePartitions {
  protected val logger = LoggerFactory.getLogger(getClass)
  val tz = TimeZone.getTimeZone("UTC")

  def getDateHour(): String = {
    val pattern = "yyyy-MM-dd-HH-mm"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    return format.format(new Date())
  }

  def getNextDateHour(): String = {
    val pattern = "yyyy-MM-dd-HH"
    val date = new Date()
    date.setTime(date.getTime() + 3600 * 1000)
    return getDateFormater(pattern).format(date)
  }

  def getDateFormater(pattern: String): SimpleDateFormat = {
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    return format
  }

  def getNextDateMonth(): String = {
    val pattern = "yyyy-MM"
    val format = new SimpleDateFormat(pattern)
    val date = new Date()
    date.setTime(date.getTime() + 3600 * 1000)
    return getDateFormater(pattern).format(date)
  }

  def main(arg: Array[String]): Unit = {

    val KEY_SPACE_NAME = "master_database"
    val TABLE_NAME = "master_collection"

    val CONFIG_FILE_PATH = arg(0)
    val CASSANDRA_HOST = arg(1)
    val CLUSTER_NAME = arg(2)
    val CASSANDRA_USERNAME = arg(3)
    val CASSANDRA_PASSWORD = arg(4)
    val HIVE_LOCATION = arg(5)
    try {
      val spark = SparkSession.builder().appName("Add-Hive-Partitions")
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate()

      spark.conf.set(CLUSTER_NAME + "/spark.cassandra.connection.host", CASSANDRA_HOST)
      spark.conf.set(CLUSTER_NAME + "/spark.cassandra.auth.username", CASSANDRA_USERNAME)
      spark.conf.set(CLUSTER_NAME + "/spark.cassandra.auth.password", CASSANDRA_PASSWORD)
      spark.conf.set(CLUSTER_NAME + "/spark.input.consistency.level", "QUORUM")
      spark.conf.set(CLUSTER_NAME + "/spark.cassandra.connection.timeout_ms", "10000")
      spark.conf.set(CLUSTER_NAME + "/spark.cassandra.read.timeout_ms", "3600000")
      spark.conf.set(CLUSTER_NAME + "/spark.cassandra.input.split.size_in_mb", "512")

      val config = ConfigFactory.parseFile(new File(CONFIG_FILE_PATH))
      val tablesList = config.getString("tables")
      val tablesArr = tablesList.split(",").map(_.trim)

      val master_collectionDF = spark.read.format("org.apache.spark.sql.cassandra")
        .options(Map(
          "cluster" -> CLUSTER_NAME,
          "table" -> TABLE_NAME,
          "keyspace" -> KEY_SPACE_NAME)).load

      val tidList = master_collectionDF.select("tid")

      val catalog = spark.catalog
      catalog.setCurrentDatabase("gobblin_parquet_hdfs_db")

      import spark.sql

      tablesArr.foreach { table =>
        tidList.collect.foreach { tid =>
          logger.info("Adding partition for table : " + table + " with tid : " + tid.getAs("tid"))

          sql("ALTER TABLE " + table + " ADD IF NOT EXISTS PARTITION(tid= '" + tid.getAs("tid")
            + "',date_month='" + getNextDateMonth + "',date_hour='" + getNextDateHour + "') LOCATION '"
            + HIVE_LOCATION + table + "/" + "tid=" + tid.getAs("tid") + "/" + "date_month="
            + getNextDateMonth + "/" + "date_hour=" + getNextDateHour + "'")
        }
      }

      spark.stop()
    } catch {
      case ex: Exception =>
        logger.error("Error in AddHivePartitions.main()" + ex, ex.getMessage)
        ex.printStackTrace()
    }
  }
}

/*create keyspace master_database  with replication = {'class':'SimpleStrategy','replication_factor':1};
create table master_collection (tid int primary key);
insert into master_collection (tid) values (106);
insert into master_collection (tid) values (102);
insert into master_collection (tid) values (104);
insert into master_collection (tid) values (204);
insert into master_collection (tid) values (206);
insert into master_collection (tid) values (306);
insert into master_collection (tid) values (406);
insert into master_collection (tid) values (506);
insert into master_collection (tid) values (606);
insert into master_collection (tid) values (706);
insert into master_collection (tid) values (505);
insert into master_collection (tid) values (1006);*/