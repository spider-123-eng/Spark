package com.spark2.cassandra

import scala.reflect.runtime.universe

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ConstantInputDStream
import com.datastax.spark.connector.streaming.toStreamingContextFunctions
import com.datastax.spark.connector.toNamedColumnRef

/**
 * Reading from Cassandra using Spark Streaming
 */
object SparkStreaming_Cassandra extends App {
  case class Employee(id: Int, name: String, salary: Int)

  val spark = SparkSession.builder().appName("Spark_Streaming_Cassandra").master("local[*]").getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "2")
  spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")

  val KEY_SPACE_NAME = "dev"
  val TABLE_NAME = "employee"

  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  val cassandraRDD = ssc.cassandraTable[Employee](KEY_SPACE_NAME, TABLE_NAME).select("id", "name", "salary")

  val dstream = new ConstantInputDStream(ssc, cassandraRDD)

  dstream.foreachRDD { rdd =>
    println("Total Records cont in DB : " + rdd.count)
    
    println(rdd.collect.mkString("\n"))
  }

  ssc.start()
  ssc.awaitTermination()

}