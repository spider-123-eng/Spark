package com.spark2.fileformats
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType };

object Simple_XMLParser1 {

  val customSchema = StructType(Array(
    StructField("_id", StringType, nullable = true),
    StructField("author", StringType, nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("genre", StringType, nullable = true),
    StructField("price", DoubleType, nullable = true),
    StructField("publish_date", StringType, nullable = true),
    StructField("title", StringType, nullable = true)))

  def main(args: Array[String]): Unit = {

    val xmlFilePath = "input/books.xml"
    val spark = SparkSession.builder().appName("Spark-XMLParsing").master("local[*]").getOrCreate()
    spark.conf.set("spark.debug.maxToStringFields", "10000000")

    val rawDataDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .option("treatEmptyValuesAsNulls", true)
      .schema(customSchema)
      .load(xmlFilePath)

    val selectedData = rawDataDF.select("author", "_id")

    selectedData.write
      .format("com.databricks.spark.xml")
      .option("rootTag", "books")
      .option("rowTag", "book")
      .save("output/newbooks.xml")

  }
}