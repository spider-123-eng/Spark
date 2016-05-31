package com.spark.usecases
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.spark.util.Utills
object OlympicsDataAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Travel-Data-Analysis").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val textFile = sc.textFile(Utills.DATA_PATH + "/olympics_data.txt")
    val olympicsDataRDD = textFile.mapPartitions(_.drop(1)) //remove the header information from the file
    val lines = olympicsDataRDD.filter { x => { if (x.toString().split(",").length >= 10) true else false } }
      .map(line => { line.toString().split(",") })

    //Find the total number of medals won by each country in swimming.
    val filteredRDD = lines.filter(x => { if (x(5).equalsIgnoreCase("swimming") && (x(9).matches(("\\d+")))) true else false })
    val results = filteredRDD.map(x => (x(2), x(9).toInt))
    val totalNoMedals = results.reduceByKey(_ + _).collect()
    println("---Total number of medals won by each country in swimming---")
    totalNoMedals.foreach(f => println(f))

    //Find the number of medals that won by India year wise.
    val filteredIndiaRDD = lines.filter(x => { if (x(2).equalsIgnoreCase("india") && (x(9).matches(("\\d+")))) true else false })
    val indiaResults = filteredIndiaRDD.map(x => (x(3), x(9).toInt))
    val indiaMedals = indiaResults.reduceByKey(_ + _).collect()
    println("---Number of medals that won by India year wise---")
    indiaMedals.foreach(f => println(f))

    //Find the total number of medals won by each country.
    val filteredLines = lines.filter(x => { if ((x(9).matches(("\\d+")))) true else false })
    val filteredResults = filteredLines.map(x => (x(2), x(9).toInt))
    val medalsCountryWise = filteredResults.reduceByKey(_ + _).collect()
    println("---Total number of medals won by each country---")
    medalsCountryWise.foreach(f => println(f))

  }
}