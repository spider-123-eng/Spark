package com.spark2.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
object Spark_To_Caasandra extends App {

  val spark = SparkSession.builder().appName("Spark_To_Caasandra").master("local[1]").getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "2")
  spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")

  val df = spark.read.format("org.apache.spark.sql.cassandra").option("table", "emp")
    .option("keyspace", "dev")
    .load()

  df.printSchema()

  df.show()

}