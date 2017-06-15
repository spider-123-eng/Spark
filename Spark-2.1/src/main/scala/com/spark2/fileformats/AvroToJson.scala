package com.spark2.fileformats

import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._
object AvroToJson {
  case class Emp(empId: Int, emp_name: String, deptId: Int, deptName: String, location: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark_ToAvro").master("local[1]").getOrCreate()
    spark.conf.set("spark.sql.avro.compression.codec", "snappy")
    import spark.implicits._

    val empDF = List(
      Emp(1, "Mike", 1, "Cloud", "BNGL"),
      Emp(2, "Shyam", 1, "Cloud", "HYD"),
      Emp(3, "Revanth", 2, "Bigdata", "BNGL"),
      Emp(4, "Raghu", 2, "Bigdata", "HYD"),
      Emp(6, "Apporva", 3, "Apac", "BNGL"),
      Emp(5, "Naga", 3, "Apac", "HYD")).toDF()

    empDF.write.avro("output/to_avro")

    val avroDF = spark.read.avro("output/to_avro")
    avroDF.show

    avroDF.coalesce(1).write.option("compression", "gzip").json("output/avro_to_json")

  }
}