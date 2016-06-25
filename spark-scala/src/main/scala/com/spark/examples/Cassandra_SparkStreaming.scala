package com.spark.examples
import org.apache.spark._
import org.apache.spark.streaming._
import com.datastax.spark.connector.streaming._
import org.apache.spark.rdd._
import org.apache.spark.streaming.dstream.ConstantInputDStream

//Reading from Cassandra using Spark Streaming
object Cassandra_SparkStreaming {
  case class Employee(Id: Int, name: String, salary: Int)

  def main(args: Array[String]) {
    val KEY_SPACE_NAME = "spark_kafka_cassandra"
    val TABLE_NAME = "employee"
    val conf = new SparkConf().setAppName("Cassandra_SparkStreaming").set("spark.cassandra.connection.host", "127.0.0.1")

    val ssc = new StreamingContext(conf, Seconds(10))

    val cassandraRDD = ssc.cassandraTable[Employee](KEY_SPACE_NAME, TABLE_NAME).select("id", "name", "salary")

    val dstream = new ConstantInputDStream(ssc, cassandraRDD)

    dstream.foreachRDD { rdd =>
      println("Total Records cont in DB : " + rdd.count)
      println(rdd.collect.mkString("\n"))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}