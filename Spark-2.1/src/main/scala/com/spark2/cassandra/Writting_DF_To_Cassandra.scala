package com.spark2.cassandra

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object Writting_DF_To_Cassandra extends App {

  case class Emp(id: Int, name: String, salary: Int)
  val spark = SparkSession.builder().appName("Spark_To_Caasandra").master("local[1]").getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "2")
  spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")

  val KEY_SPACE_NAME = "dev"
  val TABLE_NAME = "employee"

  val emps = List(
    Emp(1, "Mike", 1032230),
    Emp(2, "Shyam", 1322200),
    Emp(3, "Revanth", 2223300),
    Emp(4, "Raghu", 2773666),
    Emp(5, "naga", 2002233),
    Emp(6, "siva", 2773666))

  val empDF = spark.createDataFrame(emps)
  

  empDF.write.format("org.apache.spark.sql.cassandra").option("table", TABLE_NAME)
    .option("keyspace", KEY_SPACE_NAME).mode(SaveMode.Append).save()

  println("done .......")
}

//CREATE TABLE dev.employee (
//    id int PRIMARY KEY,
//    name text,
//    salary int
//);