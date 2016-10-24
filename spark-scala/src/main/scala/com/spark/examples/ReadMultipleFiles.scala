package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
object ReadMultipleFiles {
  case class Employee(empid: Int, name: String, dept: String, salary: Int, nop: Int)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-ReadMultipleFiles").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //Directory structure 
    // employee/folder1/emp.txt (or) employee/emp.txt,emp1.txt
    // employee/folder2/emp1.txt (or) employee/folder2/emp1.txt,emp2.txt

    val empDataRDD = sc.textFile("E:/employee/*").coalesce(1, false)

    val filteredRDD = empDataRDD.filter(line => !line.contains("empid"))

    val empDF = filteredRDD.filter { lines => lines.length() > 0 }.
      map(_.split("\\|")).
      map(p => Employee(p(0).trim.toInt, p(1), p(2), p(3).trim.toInt, p(4).trim.toInt)).toDF()

    empDF.show()

    //val empDataRDD1 = sc.wholeTextFiles("E:/test/*")
    //empDataRDD1.collect().foreach { x => println(x._2) }
  }
}