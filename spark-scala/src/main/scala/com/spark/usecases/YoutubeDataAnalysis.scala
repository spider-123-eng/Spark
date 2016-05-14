package com.spark.usecases
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
object YoutubeDataAnalysis {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Youtube-Data-Analysis").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val textFile = sc.textFile("E:/Software/Spark/data/youtubedata.txt")
    
    //Here, we will find out what are the top five categories with maximum number of videos uploaded.
    // val counts = textFile.map(line => { var YoutubeRecord = ""; val temp = line.split("\t"); ; if (temp.length >= 3) { YoutubeRecord = temp(3) }; YoutubeRecord })
    val counts = textFile.map(_.split("\t")).filter(_.length >= 3).map(_(3))
    val test = counts.map(x => (x, 1))
    val res = test.reduceByKey(_ + _).map(item => item.swap).sortByKey(false).take(5)
    res.foreach(f => println(f))

    //In this problem statement, we will find the top 10 rated videos in YouTube.
    val counts1 = textFile.filter { x => { if (x.toString().split("\t").length >= 6) true else false } }.map(line => { line.toString().split("\t") })
    val pairs = counts1.map(x => { (x(0), x(6).toDouble) })
    val res1 = pairs.reduceByKey(_ + _).map(item => item.swap).sortByKey(false).take(10)
    res1.foreach(f => println(f))

  }
}