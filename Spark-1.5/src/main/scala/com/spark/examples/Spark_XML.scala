package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
object Spark_XML {

  //Reference ---> https://github.com/databricks/spark-xml

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark_XML_Parsing").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.read.format("com.databricks.spark.xml")
      .option("rowTag", "book").load("/user/data/books.xml")

    val selectedData = df.select("author", "title", "_id")
    selectedData.show()

    //You can manually specify the schema when reading data:

    import org.apache.spark.sql.SQLContext
    import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType };

    val customSchema = StructType(Array(
      StructField("_id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true)))

    val df1 = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .schema(customSchema)
      .load("/user/data/books.xml")

    val selectedData1 = df1.select("author", "_id")
    selectedData1.show()

  }
}