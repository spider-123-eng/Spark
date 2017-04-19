package com.spark.usecases
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import com.spark.util.Utills
object TravelDataAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Travel-Data-Analysis").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val textFile = sc.textFile(Utills.DATA_PATH + "/TravelData.txt")
    val travelDataRDD = textFile.mapPartitions(_.drop(1)) //remove the header information from the file

    //Top 20 destination people travel the most  
    val top20DestinationRDD = travelDataRDD.map(lines => lines.split('\t')).
      map(x => (x(2), 1)).reduceByKey(_ + _).
      map(item => item.swap).sortByKey(false).take(20)
    top20DestinationRDD.foreach(f => println(f))

    //Top 20 locations from where people travel the most
    val top20LocationsRDD = travelDataRDD.map(lines => lines.split('\t')).map(x => (x(1), 1)).
      reduceByKey(_ + _).map(item => item.swap).sortByKey(false).take(20)

    top20LocationsRDD.foreach(f => println(f))

    //Top 20 cities that generate high airline revenues for travel
    val fil = travelDataRDD.map(x => x.split('\t')).filter(x => { if ((x(3).matches(("1")))) true else false })
    // fil.collect().foreach { x => println(x(2)) }
    val Top20Cities = fil.map(x => (x(2), 1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).take(20)
    Top20Cities.foreach(f => println(f))
  }

  //https://acadgild.com/blog/spark-use-case-travel-data-analysis/
}