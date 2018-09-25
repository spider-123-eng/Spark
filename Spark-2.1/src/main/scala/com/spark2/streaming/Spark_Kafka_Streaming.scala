package com.spark2.streaming

import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds

object Spark_Kafka_Streaming {

  def main(args: Array[String]) {
    val SLIDE_INTERVAL = 20 //in seconds
    val topics = "demo"
    val zkQuorum = "localhost:9092"
    val sc = new SparkContext(new SparkConf().setAppName("Spark-Kafka-Streaming").setMaster("local[2]"))
    val ssc = new StreamingContext(sc, Seconds(SLIDE_INTERVAL))

    val topicsSet = topics.split(",").toSet
    println("Streaming topics : " + topicsSet)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> zkQuorum)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)

    messages.foreachRDD(
      rdd => {
        if (!rdd.isEmpty()) {
          println("First record : " + rdd.first())
          println("rdd count : " + rdd.count())
        } else {
          println("Data is not yet recevied from the producer....")
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }
}