package com.spark2.jdbc

import java.sql.Date
import java.util.Calendar
import java.util.Properties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Spark_To_Jdbc {
  protected val logger = LoggerFactory.getLogger(getClass)

  case class Emp(empno: Int, ename: String, job: String, mgr: Int, hiredate: Date, sal: Long, comm: Long, deptno: Int)

  def main(arg: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder().appName("Spark_To_Jdbc")
        .master("local[*]").getOrCreate()

      //localhost demo emp root root@123
      val jdbcHostname = arg(0)
      val jdbcDatabase = arg(1)
      val tableName = arg(2)
      val jdbcUsername = arg(3)
      val jdbcPassword = arg(4)
      val jdbcPort = 3306

      val connectionProp = new Properties()
      connectionProp.put("user", jdbcUsername)
      connectionProp.put("password", jdbcPassword)

      //jdbc_url :jdbc:mysql://localhost:3306/demo
      val jdbc_url = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

      import spark.implicits._

      val date = new java.sql.Date(Calendar.getInstance().getTime().getTime())

      val empDF = Seq(
        Emp(7362, "SMITH", "CLERK", 7902, date, 1000, 0, 20),
        Emp(7363, "ALLEN", "SALESMAN", 7698, date, 1600, 0, 30)).toDF()

      //Writing data to JDBC
      empDF.coalesce(1).write.mode(SaveMode.Append).jdbc(jdbc_url, tableName, connectionProp)

      //Reading data from JDBC
      val jdbcDF = spark.read.jdbc(jdbc_url, tableName, connectionProp)
      jdbcDF.show

      //Query to Database Engine
      val dbTable = "(select empno, concat_ws(' ', ename, job) as name_job from emp) as emp_job";
      val jdbcDF1 =
        spark.read.jdbc(jdbc_url, dbTable, "empno", 10001, 499999, 10, connectionProp)
      jdbcDF1.show

      val query = "(select * from emp where sal < 10000) emp_alias"
      val df = spark.read.jdbc(jdbc_url, query, connectionProp)
      df.show

      //Managing Parallelism on JDBC Reads
      //We will split the table read across executors on the emp_no column using the partitionColumn, lowerBound, upperBound, numPartitions parameters.
      val df1 = (spark.read.jdbc(url = jdbc_url,
        tableName,
        columnName = "empno",
        lowerBound = 1L,
        upperBound = 100000L,
        numPartitions = 100,
        connectionProp))
      df1.show

    } catch {
      case ex: Exception =>
        logger.error("Error in Spark_To_Jdbc.main()" + ex, ex.getMessage)
        ex.printStackTrace()
    }
  }
}
/*  CREATE TABLE emp (
  empno int NOT NULL PRIMARY KEY,
  ename varchar(10) default NULL,
  job varchar(9) default NULL,
  mgr int default NULL,
  hiredate date default NULL,
  sal long default NULL,
  comm long default NULL,
  deptno int default NULL
	); */
