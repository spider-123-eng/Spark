package com.spark.usecases.twitteranalytics

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._

object TwitterAnalytics extends App {
  val conf = new SparkConf().setAppName("myStream").setMaster("local[2]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val ssc = new StreamingContext(sc, Seconds(2))
  val client = new twitterclient()
  val tweetauth = client.start()
  val inputDstream = TwitterUtils.createStream(ssc, Option(tweetauth.getAuthorization))

   // Split the stream on space and extract hashtags 
  val hashTags = inputDstream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

  // Get the top hashtags over the previous 60 sec window
  val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
    .map { case (topic, count) => (count, topic) }
    .transform(_.sortByKey(false))

  // Get the top hashtags over the previous 10 sec window
  val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
    .map { case (topic, count) => (count, topic) }
    .transform(_.sortByKey(false))

  // print tweets in the currect DStream 
  inputDstream.print()

  // Print popular hashtags  
  topCounts60.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
    topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
  })
  topCounts10.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
    topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
  })


  ssc.start()
  ssc.awaitTermination()
}