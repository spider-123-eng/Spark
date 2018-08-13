package com.spark2.cassandra
import org.apache.spark.sql.SparkSession

object FilterCassandraData extends App {

  case class Employee(id: Int, name: String, salary: Int)

  val spark = SparkSession.builder().appName("Spark_To_Caasandra").master("local[1]").getOrCreate()
  import spark.implicits._
  
  spark.conf.set("spark.sql.shuffle.partitions", "2")
  spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")

  val KEY_SPACE_NAME = "dev"
  val TABLE_NAME = "employee"
  val TABLE_NAME1 = "master_collection1"

  //loading data from cassandra table
  val df = spark.read.format("org.apache.spark.sql.cassandra").option("table", TABLE_NAME)
    .option("keyspace", KEY_SPACE_NAME)
    .load()
  //  df.printSchema()
  //  df.show()

  val masterdf = spark.read.format("org.apache.spark.sql.cassandra").option("table", TABLE_NAME1)
    .option("keyspace", KEY_SPACE_NAME)
    .load()
  val tidfiltDF = masterdf.select("id").where(masterdf("disable") === "0")
  tidfiltDF.show()
  val tidList = tidfiltDF.select("id").map(r => r.getInt(0)).collect.toList
  val filt = tidList.mkString("id in (", ",", ")")
  println(filt)
  
  val finalfildf =  df.filter(filt)
  finalfildf.show()
  finalfildf.select("id").distinct.show()

}