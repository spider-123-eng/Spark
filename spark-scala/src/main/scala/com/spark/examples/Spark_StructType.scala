package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.spark.util._
object Spark_StructType {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark_StructType_Example").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val people = sc.textFile(Utills.DATA_PATH +"person.txt")
    val schemaString = "firstName lastName age"

    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1), p(2).trim))
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    peopleDataFrame.registerTempTable("people")

    val results = sqlContext.sql("SELECT firstName,age FROM people")

    results.map(t => "Name: " + t(0) + "," + "Age: " + t(1)).collect().foreach(println)
  }
}