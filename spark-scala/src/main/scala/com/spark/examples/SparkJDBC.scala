package com.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SparkJDBC {

  def getDetails(sc: SparkContext): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val url = "jdbc:oracle:thin:@localhost:1521:XE"
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver");
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    val employee = sqlContext.read.jdbc(url, "emp", prop)
    employee.cache()
    employee.registerTempTable("emp")
    
    sqlContext.sql("select * from emp where NAME like 'HARI%' ").show()

    employee.select("EMPID", "NAME", "SALARY").show()

    employee.filter(employee("SALARY") > 7000).show()

    employee.groupBy("NAME").count().show()
    
    sc.stop()

  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-JDBC").setMaster("local[1]")
    val sc = new SparkContext(conf);

    getDetails(sc)

  }
}