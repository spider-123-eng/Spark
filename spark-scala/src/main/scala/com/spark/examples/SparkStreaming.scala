package com.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
object SparkStreaming {
  val SLIDE_INTERVAL = 1
  def startStreaming(args: Array[String]): Unit = {
    try {
      println("Calling ........................")
      val sc = new SparkContext(new SparkConf().setAppName("Spark-Kafka-Streaming").setMaster("local[2]"))
      val ssc = new StreamingContext(sc, Seconds(SLIDE_INTERVAL))
      val logLinesDStream = ssc.textFileStream("C:\\temp")

      logLinesDStream.foreachRDD(accessLogs => {
        if (accessLogs.count() == 0) {
          println("No logs received in this time interval")
        } else {
           println("Else No logs received in this time interval")
        }
      })

      ssc.start()
      ssc.awaitTermination()
    } catch {
      case ex: NoClassDefFoundError => {
        println(ex.printStackTrace())
      }
    }

  }
  def main(args: Array[String]) {
    /*if (args.length < 2) {
      System.err.println("Usage: KafkaConsumer <zkQuorum>  <topics> <path>")
      System.exit(1)
    }*/
    startStreaming(args)
  }
}