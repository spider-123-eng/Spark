package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object Window_Sliding_Interval {

  //nc -lk 9999

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Window_Sliding_Interval").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    ssc.checkpoint("/user/data/checkpoints/")

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))

    // Reduce last 30 seconds of data, every 10 seconds
    val windowedWordCounts = pairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(30), Seconds(10))
    windowedWordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}