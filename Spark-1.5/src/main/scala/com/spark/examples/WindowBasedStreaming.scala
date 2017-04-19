package com.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import com.spark.util._
object WindowBasedStreaming {

  //nc -lk 9999

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Window-Based-Streaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(10))

    //ssc.checkpoint("/user/data/checkpoints/")

    val lines = ssc.socketTextStream("localhost", 9999)

    val messages = lines.window(Seconds(30), Seconds(10))

    messages.foreachRDD(
      rdd => {
        if (!rdd.isEmpty()) {
          println("rdd count  " + rdd.count())
          val path = "file:///opt/home/data/" + Utills.getTime()
          rdd.coalesce(1, false).saveAsTextFile(path)
        } else {
          println("Data is not yet recevied from the producer....")
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }
}