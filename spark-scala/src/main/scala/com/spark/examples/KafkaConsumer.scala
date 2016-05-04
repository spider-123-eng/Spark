package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import java.util.Properties
import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import kafka.serializer.StringDecoder
object KafkaConsumer {

  def main(args: Array[String]) {
    try {
      val Array(brokerList, topics) = args
      val sc = new SparkContext(new SparkConf().setAppName("KafkaConsumer-Streaming").setMaster("local[2]"))
      val ssc = new StreamingContext(sc, Seconds(5))
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)

      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicsSet).map(_._2)

      messages.foreachRDD(x => {
        if (!x.isEmpty()) {
          x.foreach { x => println(x) }
          println("--------------------------------------------------------")
          println(x.first())
        }else{
          println("Data is not received from the producer")
        }
      })
      ssc.start()
      ssc.awaitTermination()

    } catch {
      case ex: Exception => {
        println(ex.printStackTrace())
      }
    }
  }
}