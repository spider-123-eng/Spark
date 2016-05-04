package com.spark.examples
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import kafka.serializer.StringDecoder
object SparkKafkaProducer {
  def startStreaming(args: Array[String]): Unit = {
    try {

      val Array(zkQuorum, topics) = args
      val sc = new SparkContext(new SparkConf().setAppName("SparkKafkaProducer").setMaster("local[2]"))
      val ssc = new StreamingContext(sc, Seconds(5))
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> zkQuorum)

      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicsSet)
      ssc.start()
      ssc.awaitTermination()

    } catch {
      case ex: Exception => {
        println(ex.printStackTrace())
      }
    }
  }
  def main(args: Array[String]) {
    startStreaming(args)
  }
}