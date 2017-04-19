package com.spark2.parquet

import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }
object FileCompression {

  case class DataFrameSample(name: String, actor: String, episodeDebut: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark File Compression Handling")
      .master("local[1]")
      .getOrCreate()

    val df = spark.createDataFrame(
      DataFrameSample("Homer", "Dan Castellaneta", "Good Night") ::
        DataFrameSample("Marge", "Julie Kavner", "Good Night") ::
        DataFrameSample("Bart", "Nancy Cartwright", "Good Night") ::
        DataFrameSample("Lisa", "Yeardley Smith", "Good Night") ::
        DataFrameSample("Maggie", "Liz Georges and more", "Good Night") ::
        DataFrameSample("Sideshow Bob", "Kelsey Grammer", "The Telltale Head") ::
        Nil).toDF().cache()

    df.write.mode("overwrite").format("parquet").option("compression", "none").mode("overwrite").save("/tmp/file_no_compression_parq")
    df.write.mode("overwrite").format("parquet").option("compression", "gzip").mode("overwrite").save("/tmp/file_with_gzip_parq")
    df.write.mode("overwrite").format("parquet").option("compression", "snappy").mode("overwrite").save("/tmp/file_with_snappy_parq")
    //lzo - requires a different method in terms of implementation.

    df.write.mode("overwrite").format("orc").option("compression", "none").mode("overwrite").save("/tmp/file_no_compression_orc")
    df.write.mode("overwrite").format("orc").option("compression", "snappy").mode("overwrite").save("/tmp/file_with_snappy_orc")
    df.write.mode("overwrite").format("orc").option("compression", "zlib").mode("overwrite").save("/tmp/file_with_zlib_orc")
  }

}