package com.spark.usecases.sensoranalytics

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.joda.time.DateTime
import com.spark.util._
object SensorAnalytics {
  val conf = new Configuration()
  val fs = FileSystem.get(conf)
  val uri = conf.get("fs.default.name")
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def calculateCountryStats(parsedDstream: DStream[SensorRecord], windowInterval: Int, slideInterval: Int): DStream[String] = {
    val countryAggregation = parsedDstream.map(eachRec => ((eachRec.dateTime, eachRec.country), 1))

    val resultStream = countryAggregation.window(Seconds(windowInterval), Seconds(slideInterval)).reduceByKey(_+_)
    resultStream.map {
      case ((dateTime: DateTime, country: String), count: Int) => dateTime.toString("yyyy-MM-dd HH:mm:ss") + "," + country + "," + count
    }
  }

  def calculateStateStats(parsedDstream: DStream[SensorRecord], windowInterval: Int, slideInterval: Int): DStream[String] = {
    val stateAggregation = parsedDstream.map(eachRec => ((eachRec.dateTime, eachRec.state), 1))

    val resultStream = stateAggregation.window(Seconds(windowInterval), Seconds(slideInterval)).reduceByKey(_+_)

    resultStream.map {
      case ((dateTime: DateTime, state: String), count: Int) => dateTime.toString("yyyy-MM-dd HH:mm:ss") + "," + state + "," + count
    }
  }

  def calculateCityStats(parsedDstream: DStream[SensorRecord], windowInterval: Int, slideInterval: Int): DStream[String] = {
    val cityAggregation = parsedDstream.map(eachRec => ((eachRec.dateTime, eachRec.city, eachRec.sensorStatus), 1))

    val resultStream = cityAggregation.window(Seconds(windowInterval), Seconds(slideInterval)).reduceByKey(_+_)

    resultStream.map {
      case ((dateTime: DateTime, city: String, sensorStatus: String), count: Int) => dateTime.toString("yyyy-MM-dd HH:mm:ss") + "," + city + "," + sensorStatus + "," + count
    }
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()

    val batchTime = args(0).toInt
    val windowTime = args(1).toInt
    val slideTime = args(2).toInt

    sparkConf.setMaster("local[2]")

    sparkConf.setAppName("SensorAnalytics")

    val ssc = new StreamingContext(sparkConf, Seconds(batchTime))

    val inputPath = uri + "/test/"
    val outPath = uri + "/out/"
    println("inputPath  ------> " + inputPath)
    val data = ssc.textFileStream(inputPath)

    val parsedDstream = data.map(SchemaParser.parse(_)).filter(_ != None).map(_.get)

    calculateCountryStats(parsedDstream, windowTime, slideTime).foreachRDD((outputRdd, time) => {
      if (!outputRdd.isEmpty())
        outputRdd.coalesce(1, false).saveAsTextFile(outPath + "/countryStats/" + Utills.getTime())
    })

    calculateStateStats(parsedDstream, windowTime, slideTime).foreachRDD((outputRdd, time) => {
      if (!outputRdd.isEmpty())
        outputRdd.coalesce(1, false).saveAsTextFile(outPath + "/stateStats/" + Utills.getTime())
    })

    calculateCityStats(parsedDstream, windowTime, slideTime).foreachRDD((outputRdd, time) => {
      if (!outputRdd.isEmpty())
        outputRdd.coalesce(1, false).saveAsTextFile(outPath + "/cityStats/" + Utills.getTime())
    })

    ssc.start()

    ssc.awaitTermination()

  }
}