package com.spark2.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset

case class Person(name: String, empId: Int)
case class Employee(empId: Int,emp_name:String)

case class Select(cols: Column*) {
  def transform(ds: DataFrame) = ds.select(cols: _*)
}

object Test {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("test").master("local[1]").getOrCreate()

    val person = Array(Person("John", 1), Person("Mike", 2))
    
        val employee = Array(Employee(1,"Aruba"))
    
    val personDf = session.createDataFrame(person)
    
    val employeeDf = session.createDataFrame(employee)
    
    val joinDf = personDf.join(employeeDf, Seq("empId"), "left")
    
    joinDf.write.partitionBy("name").parquet("output/test")
    
    
    joinDf.show()

  }
}
    