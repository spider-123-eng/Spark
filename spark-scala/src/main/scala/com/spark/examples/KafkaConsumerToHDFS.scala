package com.spark.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.serializer.StringDecoder
object KafkaConsumerToHDFS {
  private val conf = new Configuration()
  val fs = FileSystem.get(conf)
  private val hdfsCoreSitePath = new Path("/home/centos/hadoop-2.6.0/etc/hadoop/core-site.xml")
  conf.addResource(hdfsCoreSitePath)
  val uri = conf.get("fs.default.name")
  val SLIDE_INTERVAL = 1
  def startStreaming(args: Array[String]): Unit = {
    try {
      val zkQuorum = "10.220.11.171:9092";
      val topics = "demotest";
      val sc = new SparkContext(new SparkConf().setAppName("Spark-Kafka-Streaming").setMaster("local[2]"))
      val ssc = new StreamingContext(sc, Minutes(SLIDE_INTERVAL))
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> zkQuorum)
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicsSet).map(_._2)
      println("Messages.count()" + messages.count())
      messages.foreachRDD(
        rdd => {
          if (!rdd.isEmpty()) {
            println(rdd.first())
            println("rdd count  " + rdd.count())
            println("URI = " + uri)
            val hdfsPath = uri + "/user/data/" 
            println("HDFS Path = " + hdfsPath)
            rdd.saveAsTextFile(hdfsPath)
          } else {
            println("Data is not yet recevied from the producer....")
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