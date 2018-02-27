package com.spark2.fileformats
import org.apache.spark.sql.SQLContext
import com.databricks.spark.xml._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql
object XMLParsing {
//spark-submit --class com.spark2.fileformats.XMLParsing --master local[*] Spark-2.1-1.0.jar file:////home/centos/revanth/one.xml
  def main(args: Array[String]): Unit = {
    
    val xmlFilePath = args(0)
    val spark = SparkSession.builder().appName("XMLParsing").getOrCreate()
    spark.conf.set("spark.debug.maxToStringFields", "10000000")

    import spark.implicits._

    val df = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "us-bibliographic-data-grant")
      .option("treatEmptyValuesAsNulls", true)
      .load(xmlFilePath)

    val q1 = df.withColumn("country", $"publication-reference.document-id.country".cast(sql.types.StringType))
      .withColumn("document_number", $"publication-reference.document-id.doc-number".cast(sql.types.StringType)).select("country", "document_number")
    for (l <- q1) {
      val m1 = l.get(0)
      val m2 = l.get(1)
      println(m1, m2)
    }
  }

}