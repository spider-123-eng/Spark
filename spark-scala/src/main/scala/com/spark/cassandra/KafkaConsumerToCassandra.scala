package com.spark.cassandra

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions

import kafka.serializer.StringDecoder
object KafkaConsumerToCassandra {
  val SLIDE_INTERVAL = 1
  def startStreaming(args: Array[String]): Unit = {
    try {
      val Array(zkQuorum, topics) = args
      val sc = new SparkContext(new SparkConf().setAppName("Spark-Kafka-Streaming").setMaster("local[2]").set("spark.cassandra.connection.host", "127.0.0.1"))
      val ssc = new StreamingContext(sc, Minutes(SLIDE_INTERVAL))
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> zkQuorum)
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicsSet).map(_._2).map(line => line.split('|'))

      val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
      //Creating Session object
      val session = cluster.connect()
      session.execute("CREATE KEYSPACE IF NOT EXISTS spark_kafka_cassandra WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
      val query = "CREATE TABLE IF NOT EXISTS spark_kafka_cassandra.employee (id int PRIMARY KEY,name VARCHAR, salary int);"
      //Executing the query
      session.execute(query)

      messages.foreachRDD(
        rdd => {
          if (!rdd.isEmpty()) {
            println(rdd.first())
            println("rdd count  " + rdd.count())
            val resRDD = rdd.map(line => (line(0), line(1), line(2)))
              .saveToCassandra("spark_kafka_cassandra", "employee", SomeColumns("id", "name", "salary"))
          } else {
            println("Data is not yet recevied from the producer....")
          }
        })
      ssc.start()
      ssc.awaitTermination()
    } catch {
       case ex: Exception => {
        println(ex.getMessage)
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