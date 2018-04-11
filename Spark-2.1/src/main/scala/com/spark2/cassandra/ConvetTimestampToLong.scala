package com.spark2.cassandra

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions.{ col, udf }
import java.sql.Timestamp
object ConvetTimestampToLong extends App {

  val spark = SparkSession.builder().appName("ConvetTimestampToLong-Job").master("local[1]")
    .config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()

  var testDF = (spark.read.format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "test", "keyspace" -> "dev"))
    .load)

  println("schema and data before conversion....")
  testDF.printSchema()
  testDF.show(3, false)

  /* convert Spark Timestamp column type to Long in epoch-msecs */
  protected val udfTimestampToEpochMsecLong = udf { (ts: Timestamp) =>
    Option(ts) match {
      case Some(ts) => Some(ts.getTime())
      case _        => None
    }
  }

  val newTestDF = testDF.dtypes

  //converting all the timestamp columns in the dataframe to long type
  newTestDF.foreach { f =>
    val columnName = f._1
    val columnType = f._2
        /* for consistency with Parquet schema, convert it to Long (in epoch-millisecs).
     		* -> Note: DO NOT directly cast to long, that returns epoch-seconds, which is 3 digits shorter! */
    if (columnType.equals("TimestampType")) {
      testDF = testDF.withColumn(columnName, udfTimestampToEpochMsecLong(col(columnName)))
    }
  }
  println("schema and data after conversion....")
  testDF.printSchema()
  testDF.show(3, false)
}
//CREATE TABLE TEST (ID TEXT, NAME TEXT, VALUE TEXT, LAST_MODIFIED_DATE TIMESTAMP,CREATED_DATE timestamp, PRIMARY KEY (ID));
//INSERT INTO TEST (ID, NAME, VALUE, LAST_MODIFIED_DATE,CREATED_DATE) VALUES ('1', 'orange', 'fruit', toTimestamp(now()),toTimestamp(now()));
//INSERT INTO TEST (ID, NAME, VALUE, LAST_MODIFIED_DATE,CREATED_DATE) VALUES ('2', 'elephant', 'animal', toTimestamp(now()),toTimestamp(now()));