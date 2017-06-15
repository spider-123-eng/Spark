package com.spark2.fileformats

import org.apache.spark.sql.SparkSession

object ToParquet {
  case class Emp(empId: Int, emp_name: String, deptId: Int, deptName: String, location: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark_ToAvro").master("local[1]").getOrCreate()
    spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
    import spark.implicits._

    val empDF = List(
      Emp(1, "Mike", 1, "Cloud", "BNGL"),
      Emp(2, "Shyam", 1, "Cloud", "HYD"),
      Emp(3, "Revanth", 2, "Bigdata", "BNGL"),
      Emp(4, "Raghu", 2, "Bigdata", "HYD"),
      Emp(6, "Apporva", 3, "Apac", "BNGL"),
      Emp(5, "Naga", 3, "Apac", "HYD")).toDF()

    empDF.coalesce(1).write.parquet("output/to_parquet")

    val parquetDF = spark.read.parquet("output/to_parquet")
    parquetDF.show

  }
}