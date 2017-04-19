package com.spark.usecases
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.spark.util.Utills
object TVShowDataAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TV-Show-Data-Analysis").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val format = new java.text.SimpleDateFormat("MM/dd/yy")
    val textFile = sc.textFile(Utills.DATA_PATH + "/dialy_show_guests.txt")
    val tvDataRDD = textFile.mapPartitions(_.drop(1)) //remove the header information from the file

    //Find the top 5 kinds of GoogleKnowlege_Occupation people guested the show in a particular time period.

    val splitedRDD = tvDataRDD.map(line => line.split(","))
    val pair = splitedRDD.map(line => (line(1), format.parse(line(2))))
    val fil = pair.filter(x => { if (x._2.after(format.parse("1/11/99")) && x._2.before(format.parse("6/11/99"))) true else false })
    val top5GuestsRDD = fil.map(x => (x._1, 1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).take(5)

    top5GuestsRDD.foreach(f => println(f))
  }
}