package com.spark2.fileformats
import org.apache.spark.sql.SparkSession

object Simple_XMLParser {

  def main(args: Array[String]): Unit = {

    val xmlFilePath = args(0)
    val spark = SparkSession.builder().appName("Spark-XMLParsing").master("local[*]").getOrCreate()
    spark.conf.set("spark.debug.maxToStringFields", "10000000")

    val rawDataDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .option("treatEmptyValuesAsNulls", true)
      .load(xmlFilePath)

    println("Total books count : " + rawDataDF.count())

    val selectedData = rawDataDF.select("author", "_id")

    selectedData.show(10, false)

  }

}