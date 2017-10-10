package com.spark2.cassandra.export

import java.io.File
import java.io.FileInputStream
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

/**
 * This Object is used to backup the data from Cassandra tables.
 * @author revanthreddy
 */
object Export_Cassandra_Table_Data {

  protected val logger = LoggerFactory.getLogger(getClass)

  val spark = SparkSession.builder().appName("Export-Cassandra-Data-Job")
    .getOrCreate()

  private final val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private var inputDate: Date = _
  private var dateHour: String = _
  private var dateMonth: String = _

  /**
   * Method to set the input date from nominal date
   */
  def setInputDate(date: String): Unit = {
    inputDate = dateFormat.parse(date)
  }
  /**
   * Method to set dateMonth
   */
  def setDateMonth(date: Date): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM")
    dateMonth = dateFormat.format(date)
  }

  /**
   * This method is used to return the start date based on hours
   * @param hours
   * @return String : returns the start date
   */
  def getStartDate(hours: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(inputDate);
    cal.add(Calendar.HOUR, -hours)
    dateHour = dateFormat.format(cal.getTime())
    setDateMonth(cal.getTime())
    dateHour + " 00:00"
  }

  /**
   * This method is used to export data for the given Cassandra table
   */
  def exportCassandraData(table: String, keyspace: String, cluster: String, startTime: String, endTime: String, outputPath: String): Unit = {
    try {
      val insightDF = (spark.sqlContext
        .read.format("org.apache.spark.sql.cassandra")
        .options(Map(
          "cluster" -> cluster,
          "table" -> table,
          "keyspace" -> keyspace))
        .load)

      //filter the data based on the provided date range
      val filtInsightDF = insightDF.filter("timestamp >= cast('" + startTime + "' as timestamp) and timestamp < cast('" + endTime + "' as timestamp)")

      val outputLocation = outputPath + "/date_month=" + dateMonth + "/date_hour=" + dateHour + "-00"
      logger.info("Started writting data to outputPath : " + outputLocation)

      filtInsightDF.write.mode(SaveMode.Overwrite).partitionBy("tid")
        .parquet(outputLocation)
    } catch {
      case ex: Exception =>
        logger.error("Error in Export_Cassandra_Table_Data.exportCassandraData " + ex, ex.getMessage)
        throw new RuntimeException("Error occured while exporting the data. Failing job", ex);
    }
  }

  def showUsage(): Unit = {
    println("args: --cassandraHost <cassandraHost> --cassandraUserName <cassandraUserName> --cassandraPassword <cassandraPassword> --basePath <basePath> --nominalTime <nominalTime> --fileName <fileName>")
    throw new RuntimeException("Invalid Input")
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

      if (cassandraHost == None || cassandraUserName == None || cassandraPassword == None
        || basePath == None || nominalTime == None || fileName == None) {
        showUsage
      }
      val endTime = nominalTime.get + " 00:00"
      setInputDate(nominalTime.get)

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
        val outputPath = new Path(Paths.get(basePath.get, prop.output_location).toString()).toString()

        //Start exporting the data
        exportCassandraData(tableName, keyspaceName, clusterName.get, startTime, endTime, outputPath)
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
    } catch {
      case ex: Exception =>
        logger.error("Error in Export_Cassandra_Table_Data_Job.main() " + ex, ex.getMessage)
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
--fileName cassandra-table-export.yml*/
 
