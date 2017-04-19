package com.spark.examples
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import scala.util.Random
object KafkaProducer {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-Kafka-Producer").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val Array(zkQuorum, topic) = args
    val props: Properties = new Properties()
    // props.put("metadata.broker.list", "10.220.11.171:9092")
    props.put("metadata.broker.list", zkQuorum)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    var events = 0;
    var totalEvents = 10;
    // for loop execution with a range
    for (index <- 1 to totalEvents) {
      val salary = Random.nextInt(500000);
      val empId = Random.nextInt(1000);
      val empName = "Revanth-" + empId
      val msg = empId + "|" + empName + "|" + salary;
      producer.send(new KeyedMessage[String, String](topic, msg))
    }
  }
}