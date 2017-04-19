package com.spark2.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object Filter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Filter Example")


    val sc = new SparkContext(conf)
    println("------------ Printing Spark configs ------------")
    
    sc.getConf.getAll.foreach(f => println(f))
    val x = sc.parallelize(List("Transformation demo", "Test demo", "Filter demo", "Spark is powerfull", "Spark is faster", "Spark is in memory"))
    val lines1 = x.filter(line => line.contains("Spark") || line.contains("Transformation"))
    println("\n")
    println("---------------------------------------------")
    lines1.collect().foreach { line => println(line) }

    val lines = x.filter(line => !line.contains("Filter"))
    println("---------------------------------------------")
    lines.collect().foreach { line => println(line) }
    println("---------------------------------------------")
    val count = x.filter(line => line.contains("Spark")).count()
    println("count is : " + count)
  }
}