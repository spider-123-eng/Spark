package com.spark.examples

import scala.reflect.runtime.universe
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object DataFrame_Joins {

  case class Employee(empId: Int, empName: String, deptId: Int, salary: Int, location: String)
  case class Dept(deptId: Int, deptName: String, location: String)
  case class FinalResult(empId: Int, empName: String, deptId: Int, deptName: String, salary: Int)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-DataFrame-Joins").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //populating employee object
    val emp = sc.parallelize(Seq(
      Employee(1, "Revanth", 1, 100, "BLR"),
      Employee(2, "Shyam", 1, 200, "BLR"),
      Employee(3, "Ravi", 2, 300, "AP"),
      Employee(4, "Ganesh", 2, 400, "AP"),
      Employee(5, "Revanth Reddy", 1, 9000, "BLR"),
      Employee(6, "Hari", 2, 500, "BLR"),
      Employee(7, "Hari Prasad", 2, 5500, "BLR")))

    //populating department object
    val dept = sc.parallelize(Seq(
      Dept(1, "IT", "BLR"),
      Dept(2, "FINANCE", "BLR"),
      Dept(3, "SALES", "AP")))

    //converting employee object to Data Frame.
    val empDF = emp.map(x => Employee(x.empId, x.empName, x.deptId, x.salary, x.location)).toDF().cache()

    //converting department object to Data Frame.
    val deptDF = dept.map(x => Dept(x.deptId, x.deptName, x.location)).toDF()

    //join two spark data frames on multiple columns
    //List all the employees belongs to the same department and same location.
    val resDF = empDF.join(deptDF, (empDF("deptId") === deptDF("deptId")) && (empDF("location") === deptDF("location")))

    // print the schema to decide in which columns you are interested in the final result.
    resDF.printSchema()

    //finally we are interested in only few select columns 
    //select empId,empName,deptId,deptName,salary which matches to the above criteria ..
    val finalDF = resDF.map(x => FinalResult(x.getInt(0), x.getString(1), x.getInt(2), x.getString(6), x.getInt(3))).toDF()
    //finalDF.printSchema()

    finalDF.show()

    //Wild card search on the Data frames in 2 ways ..

    //option 1
    empDF.filter(col("empName").like("%Revanth%")).show()

    //option 2
    empDF.registerTempTable("emp")
    sqlContext.sql("select * from emp where empName like '%Hari' ").show()

    //sorting ,orderBy on Data Frames
    empDF.sort($"salary".asc).show()
    empDF.sort($"salary".desc).show()
    empDF.orderBy("empName").show() // ascending
    empDF.orderBy($"empName".desc).show()

    //dropping a column from the data frame 
    empDF.drop("empName").show()
  }
}