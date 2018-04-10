package com.spark2.cassandra

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType

object ChangeDFTypes extends App {

  val spark = SparkSession.builder().appName("ChangeDFTypes-Job").master("local[1]")
    .config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()

  var testDF = (spark.read.format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "test", "keyspace" -> "dev"))
    .load)

  println("schema and data before conversion....")
  testDF.printSchema()
  testDF.show(3, false)

  val newTestDF = testDF.dtypes

  //converting all the timestamp columns in the dataframe to long type
  newTestDF.foreach { f =>
    val columnName = f._1
    val columnType = f._2

    if (columnType.equals("TimestampType")) {
      testDF = testDF.withColumn(columnName, testDF(columnName).cast(LongType))

    }
  }
  println("schema and data after conversion....")
  testDF.printSchema()
  testDF.show(3, false)
}
//CREATE TABLE TEST (ID TEXT, NAME TEXT, VALUE TEXT, LAST_MODIFIED_DATE TIMESTAMP,CREATED_DATE timestamp, PRIMARY KEY (ID));
//INSERT INTO TEST (ID, NAME, VALUE, LAST_MODIFIED_DATE,CREATED_DATE) VALUES ('1', 'orange', 'fruit', toTimestamp(now()),toTimestamp(now()));
//INSERT INTO TEST (ID, NAME, VALUE, LAST_MODIFIED_DATE,CREATED_DATE) VALUES ('2', 'elephant', 'animal', toTimestamp(now()),toTimestamp(now()));