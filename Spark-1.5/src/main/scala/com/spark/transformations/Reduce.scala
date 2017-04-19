package com.spark.transformations
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object Reduce {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Reduce-Example").setMaster("local[1]")
    val sc = new SparkContext(conf)

    /*val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    distData.collect().foreach { x => println(x) }
    val red = distData.reduce((a, b) => a + b)
    println(red)*/

    val distFile = sc.textFile("F:\\Software\\Spark\\input.txt")

    val fil = distFile.map { x => x.split(" ").size }
    val rdd = distFile.reduce((a, b) => a + b)
    println(rdd)

    val res = distFile.map(s => s.length).reduce((a, b) => a + b)
    val res1 = distFile.reduce((a, b) => a + b)
    println(res)
    println(res1)

  }
}