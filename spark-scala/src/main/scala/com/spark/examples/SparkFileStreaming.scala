package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import StreamingContext._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
object SparkFileStreaming {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Spark-FileStreaming").setMaster("local[2]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // Create the FileInputDStream on the directory and use the
    val lines = ssc.textFileStream("hdfs://sandbox.hortonworks.com:8020/user/data/")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}