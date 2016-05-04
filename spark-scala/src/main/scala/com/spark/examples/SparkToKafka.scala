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

object SparkToKafka {

  def startStreaming(args1: Array[String]): Unit = {
    try {

      val Array(zkQuorum, topics) = args1
      val sc = new SparkContext(new SparkConf().setAppName("fm-alarm-Streaming").setMaster("local[2]"))
      val ssc = new StreamingContext(sc, Seconds(5))
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> zkQuorum)

      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicsSet)
      val dataFromKafka = getDataFromKafka(messages)
      println("data from kafka=======================" + dataFromKafka)
      //writeToKafka(dataFromKafka)
      ssc.start()
      ssc.awaitTermination()

    } catch {
      case ex: Exception => {
        println(ex.printStackTrace())
      }
    }
  }

  def getDataFromKafka(messages: InputDStream[(String, String)]): String = {
    var content_2: String = null
    messages.foreachRDD(rdd => {

      rdd.foreach(content => {

        content_2 = content._2;

        println("content from kafka is :::::" + content_2)

        val props: Properties = new Properties()
        props.put("metadata.broker.list", "168.127.49.29:6667")
        props.put("serializer.class", "kafka.serializer.StringEncoder")

        val config = new ProducerConfig(props)
        val producer = new Producer[String, String](config)

        val msg = "Message from spark to kafka" + content_2;
        producer.send(new KeyedMessage[String, String]("test", msg))
        producer.close();

      })
    })
    return content_2
  }

  def writeToKafka(fromProducer: String): Unit = {
    val props: Properties = new Properties()
    props.put("metadata.broker.list", "168.127.49.29:6667")
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    val msg = "Message from spark to kafka" + fromProducer;
    producer.send(new KeyedMessage[String, String]("test", msg))
    producer.close();
  }

  def main(args: Array[String]) {
    startStreaming(args)
  }

}