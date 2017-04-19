package com.spark.usecases
import org.apache.spark.SparkConf
import com.spark.util.Utills
import org.apache.spark.SparkContext
object OlaDataAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ola-Cab-Data-Analysis").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val dataset = sc.textFile(Utills.DATA_PATH + "/olaCabData.txt")
    val header = dataset.first()
    val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
    var days = Array("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
    val eliminate = dataset.filter(line => line != header)

    //Find the days on which each basement has more trips.
    
    val split = eliminate.map(line => line.split(",")).map { x => (x(0), format.parse(x(1)), x(3)) }
    split.foreach(f => println(f))
    
    val combine = split.map(x => (x._1 + " " + days(x._2.getDay), x._3.toInt))
    combine.foreach(f => println(f))
    
    val arrange = combine.reduceByKey(_ + _).map(item => item.swap).sortByKey(false).collect.foreach(println)
  }
}