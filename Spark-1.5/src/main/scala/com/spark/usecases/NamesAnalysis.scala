package com.spark.usecases
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.spark.util.Utills
object NamesAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Names-Analysis").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val babyNamesRDD = sc.textFile(Utills.DATA_PATH + "/baby_names.txt")

    //remove the header information from the file
    val dropHeaderRDD = babyNamesRDD.mapPartitions(_.drop(1))
    val rows = dropHeaderRDD.map(line => line.split(","))

    //unique counties over the years of data collect
    val count = rows.map(row => row(2)).distinct.count
    println("unique counties count --> " + count)
    //rows.collect().foreach { x => println(x(0) + " : " +x(1) + " : " + x(2) + " : " + x(3) + " : " + x(4)) }

    //rows containing the name "SACHIN"
    val sachinRows = rows.filter(row => row(1).contains("SACHIN"))

    //Number of rows where NAME "SACHIN" has a "Count" greater than 10
    sachinRows.filter(row => row(4).toInt > 10).count()

    val uniqueCounties = sachinRows.filter(row => row(4).toInt > 10).map(r => r(2)).distinct
    println("-------- unique country names which have had the name 'SACHIN' ---------")
    uniqueCounties.foreach { x => println(x) }
    // unique counties which have had the name SACHIN over 10 times in a given year
    val uniCountryCount = sachinRows.filter(row => row(4).toInt > 10).map(r => r(2)).distinct.count
    println("unique counties which have had the name SACHIN --> " + uniCountryCount)

    val names = rows.map(name => (name(1), 1))
    // shows number of times each name appears in file
    names.reduceByKey((a, b) => a + b).sortBy(_._2).foreach(println _)

    //Another way to filter the header information
    val filteredRows = babyNamesRDD.filter(line => !line.contains("Count")).map(line => line.split(","))
    filteredRows.map(n => (n(1), n(4).toInt)).reduceByKey((a, b) => a + b).sortBy(_._2).foreach(println _)
  }
}