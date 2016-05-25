package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import com.spark.util._
object DataFrame {
  case class Employee(empid: Int, name: String, dept: String, salary: Int, nop: Int)
  case class AggregatedEmpData(empid: Int, name: String, dept: String, sumsalary: Long, sumnop: Long, maxsalary: Int, date: String)
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-DataFrame").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val empDataRDD = sc.textFile("E:/Software/Spark/data/emp.txt") //path to emp.txt
    val dropHeaderRDD = empDataRDD.mapPartitions(_.drop(1)) //remove the header information from the file

    /*println(dropHeaderRDD.first())

    val df = empDataRDD.toDF("line")
    val errors = df.filter(col("line").like("%Revanth1%"))
    println(errors.count())*/

    val empDF = dropHeaderRDD.filter { lines => lines.length() > 0 }.
      map(_.split("\\|")).
      map(p => Employee(p(0).trim.toInt, p(1), p(2), p(3).trim.toInt, p(4).trim.toInt)).toDF()

    empDF.show()

    //Spark Aggregations 
    val aggDF = empDF.groupBy("empid", "name", "dept").
      agg(sum(empDF.col("salary")), sum(empDF.col("nop")), max(empDF.col("salary")))
    aggDF.printSchema()

    //Adding extra column at the end ..
    val finalDF = aggDF.map(row => AggregatedEmpData(row.getInt(0), row.getString(1), row.getString(2), row.getLong(3), row.getLong(4), row.getInt(5), Utills.getTime()))
    println(finalDF.first())

    //Saving data as text file
    aggDF.rdd.coalesce(1, false).saveAsTextFile("F:/Software/Spark/data/aggData/" + Utills.getTime())

    empDF.groupBy("empid").agg(max(empDF.col("salary"))).show()
    empDF.select(max($"salary")).show()

  }
}