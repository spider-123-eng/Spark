package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import com.spark.util._
object Spark_Avro {
  case class Employee(empid: Int, name: String, dept: String, salary: Int, nop: Int)
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-Avro").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // configuration to use deflate compression
    sqlContext.setConf("spark.sql.avro.compression.codec", "deflate")
    sqlContext.setConf("spark.sql.avro.deflate.level", "5")
    import sqlContext.implicits._

    val empDataRDD = sc.textFile(Utills.DATA_PATH + "emp.txt") //path to emp.txt
    val dropHeaderRDD = empDataRDD.mapPartitions(_.drop(1)) //remove the header information from the file

    val empDF = dropHeaderRDD.filter { lines => lines.length() > 0 }.
      map(_.split("\\|")).
      map(p => Employee(p(0).trim.toInt, p(1), p(2), p(3).trim.toInt, p(4).trim.toInt)).toDF()

    empDF.show()

    //write as avro file.
    empDF.write.avro("/user/data/Emp_avro")

    //reading from avro file.
    val df = sqlContext.read.avro("/user/data/Emp_avro")
    df.filter("salary > 1000").show()

    //Writing Partitioned Data
    val moviesDF = Seq(
      (2012, 8, "Batman", 9.8),
      (2012, 8, "Hero", 8.7),
      (2012, 7, "Robot", 5.5),
      (2011, 7, "Git", 2.0)).toDF("year", "month", "title", "rating")

    moviesDF.write.partitionBy("year", "month").avro("/user/data/movies")

    //Reading Partitioned Data
    val resultDF = sqlContext.read.avro("/user/data/movies")
    resultDF.printSchema()
    resultDF.filter("year = 2011").collect().foreach(println)
  }
}