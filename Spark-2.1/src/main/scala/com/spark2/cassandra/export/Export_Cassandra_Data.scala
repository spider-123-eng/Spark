package com.spark2.cassandra.export
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.annotation.migration
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.util.Date
import java.io.FileInputStream
import org.yaml.snakeyaml.Yaml
import java.io.File
import org.yaml.snakeyaml.constructor.Constructor
import org.apache.spark.sql.functions._
import java.nio.file.Paths
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.slf4j.helpers.MessageFormatter
import java.io.IOException
import org.apache.spark.sql.SaveMode

/**
 * This Object is used to backup the data from Cassandra tables.
 * @author revanthreddy
 */
object Export_Cassandra_Data {

  protected val logger = LoggerFactory.getLogger(getClass)

  val spark = SparkSession.builder().appName("Export-Cassandra-Data-Job")
    .getOrCreate()

  private final val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private var inputDate: Date = _
  private var jobId = None: Option[String]
  private final var tempDir: Path = _
  private final var tempBackupDir: Path = _
  val pathsListToMove = new collection.mutable.HashMap[Path, Path]()
  private final val TEMP_BASE_PATH = "backup-temp"
  private final val TEMP_BACKUP_OUTPUT_PATH = "backup"
  private val conf = new Configuration()
  private val fs = FileSystem.get(conf)

  /**
   * Method to set the nominal date
   */
  def setInputDate(date: String): Unit = {
    inputDate = dateFormat.parse(date)
  }

  /**
   * UDF's to parse the timeStamp
   */
  val getDateMonth = udf(Utils.setDateMonth)
  val getDateHour = udf(Utils.setDateHour)

  /**
   * This method is used to return the start date based on hours
   * @param hours
   * @return String : returns the start date
   */
  def getStartDate(hours: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(inputDate);
    cal.add(Calendar.HOUR, -hours)
    dateFormat.format(cal.getTime()) + " 00:00"
  }

  /**
   * This method is used to export data for the given Cassandra table
   */
  def exportCassandraData(table: String, keyspace: String, cluster: String, startTime: String, endTime: String): Unit = {
    try {
      import spark.implicits._
      val insightDF = (spark.sqlContext
        .read.format("org.apache.spark.sql.cassandra")
        .options(Map(
          "cluster" -> cluster,
          "table" -> table,
          "keyspace" -> keyspace))
        .load)

      //filter the data based on the provided date range
      val filtInsightDF = insightDF.filter("timestamp >= cast('" + startTime + "' as timestamp) and timestamp < cast('" + endTime + "' as timestamp)")

      val finalInsightDF = filtInsightDF
        .withColumn("date_month", getDateMonth(filtInsightDF.col("timestamp")))
        .withColumn("date_hour", getDateHour(filtInsightDF.col("timestamp")))

      logger.info("Count of records : " + finalInsightDF.count())
      logger.info("Started writting to temp location : " + tempBackupDir.toString())
      if (finalInsightDF.count() > 0) {
        logger.info("first record : " + finalInsightDF.first())
        finalInsightDF.write.mode(SaveMode.Overwrite).partitionBy("tid", "date_month", "date_hour")
          .parquet(tempBackupDir.toString())
      }
    } catch {
      case ex: Exception =>
        logger.error("Error in Export_Cassandra_Table_Data.exportCassandraData " + ex, ex.getMessage)
        throw new RuntimeException("Error occured while exporting the data. Failing job", ex);
    }
  }

  def showUsage(): Unit = {
    println("args: --cassandraHost <cassandraHost> --cassandraUserName <cassandraUserName> --cassandraPassword <cassandraPassword> --basePath <basePath> --nominalTime <nominalTime> --fileName <fileName> --jobId <jobId>")
    throw new RuntimeException("Invalid Input")
  }

  def createTempDirectories(): Unit = {
    if (fs.exists(tempDir)) {
      logger.warn("Temp directory : {} exists. Deleting it first" + tempDir)
      fs.delete(tempDir, true)
    }
    logger.info("Creating temp directories at location :" + tempDir)
    fs.mkdirs(tempDir)
  }

  def deleteTempDirectories(): Unit = {
    logger.info("Deleting temp directory at location :" + tempDir)
    fs.delete(tempDir, true)
  }

  /**
   * Method to move the data from temporary location to backup directory
   */
  @throws(classOf[IOException])
  def moveData(): Unit = {
    if (!pathsListToMove.isEmpty) {
      for ((tempBackupDir, outputPath) <- pathsListToMove) {
        val moveData = fs.rename(tempBackupDir, outputPath)
        logger.info("Moving data from :" + tempBackupDir + " to outputPath  :" + outputPath)
        if (!moveData) {
          val message = "Error in moving from :" + tempBackupDir + " to outputPath  :" + outputPath
          logger.error(message)
          throw new IOException(message)
        }
      }
    }
  }

  def run(arg: Array[String]): Unit = {
    try {
      val args = Utils.argsParser(arg)
      val cassandraHost = Option(args.get("cassandraHost"))
      val clusterName = Option(args.get("cassandraUserName"))
      val cassandraUserName = Option(args.get("cassandraUserName"))
      val cassandraPassword = Option(args.get("cassandraPassword"))
      val basePath = Option(args.get("basePath"))
      val nominalTime = Option(args.get("nominalTime"))
      val fileName = Option(args.get("fileName"))
      jobId = Option(args.get("jobId"))

      if (cassandraHost == None || cassandraUserName == None || cassandraPassword == None
        || basePath == None || nominalTime == None || fileName == None || jobId == None) {
        showUsage
      }
      val endTime = nominalTime.get + " 00:00"
      setInputDate(nominalTime.get)

      //creating temporary directories
      tempDir = new Path(Paths.get(TEMP_BASE_PATH, jobId.get).toString())
      createTempDirectories

      spark.conf.set(clusterName.get + "/spark.cassandra.connection.host", cassandraHost.get)
      spark.conf.set(clusterName.get + "/spark.cassandra.auth.username", cassandraUserName.get)
      spark.conf.set(clusterName.get + "/spark.cassandra.auth.password", cassandraPassword.get)
      spark.sqlContext.setConf(clusterName.get + "/spark.input.consistency.level", "QUORUM")
      spark.sqlContext.setConf(clusterName.get + "/spark.cassandra.connection.timeout_ms", "10000")
      spark.sqlContext.setConf(clusterName.get + "/spark.cassandra.read.timeout_ms", "3600000")
      spark.sqlContext.setConf(clusterName.get + "/spark.cassandra.input.split.size_in_mb", "512")

      //parsing the cassandra-table-export.yml file
      val ymlFileInputStream = new FileInputStream(new File(fileName.get))
      val yaml = new Yaml(new Constructor(classOf[CassandraYaml]))
      val obj = yaml.load(ymlFileInputStream).asInstanceOf[CassandraYaml]
      val yamlPropsArry = obj.getCassandra_table_export()
      val yamlPropsArryItr = yamlPropsArry.iterator()

      while (yamlPropsArryItr.hasNext()) {
        val prop = yamlPropsArryItr.next()
        val durationInHours = prop.duration_in_hour.toInt
        val startTime = getStartDate(durationInHours)
        val tableName = prop.table_name
        val keyspaceName = prop.keyspace
        tempBackupDir = new Path(Paths.get(tempDir.toString(), tableName).toString())
        val outputPath = new Path(Paths.get(basePath.get, prop.output_location).toString())
        pathsListToMove.put(tempBackupDir, outputPath)
        exportCassandraData(tableName, keyspaceName, clusterName.get, startTime, endTime)
      }
    } catch {
      case ex: Exception =>
        logger.error("Error in Export_Cassandra_Table_Data_Job " + ex, ex.getMessage)
        throw new RuntimeException("Error occured while exporting the data. Failing job", ex)
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      run(args)
      moveData()
      deleteTempDirectories

    } catch {
      case ex: Exception =>
        logger.error("Error in Export_Cassandra_Table_Data_Job " + ex, ex.getMessage)
        throw new RuntimeException("Error occured while exporting the data. Failing job", ex)
    }
  }
}


/*spark-submit --class com.spark2.cassandra.Export_Cassandra_Table_Data --master yarn 
--deploy-mode cluster --conf spark.yarn.driver.memoryOverhead=8g --driver-memory 2g 
--executor-memory 4g --num-executors 4 --executor-cores 4 --conf spark.yarn.executor.memoryOverhead=2g 
--conf spark.cassandra.input.consistency.level=QUORUM --queue jobs 
--files hdfs://xxx.xx.xx.xx:9000/user/centos/oozie/cassandra-table-export/cassandra-table-export.yml 
cassandra-table-export-0.1.0-SNAPSHOT.jar --cassandraHost xxx.xx.xx.xx --cassandraUserName xxxx 
--cassandraPassword xxxxx --basePath /user/centos/ --nominalTime 2017-09-24 
--fileName cassandra-table-export.yml --jobId test123 */