package com.spark2.dataframes

import org.apache.spark.sql.SparkSession
/**
 * Partition the data by a specific column and store it partition wise.
 */
object PartitionByColumn {

  case class Emp(empId: Int, emp_name: String, deptId: Int, deptName: String, location: String)

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("PartitionByColumn").master("local[1]").getOrCreate()

    val emps = List(
      Emp(1, "Mike", 1, "Cloud", "BNGL"),
      Emp(2, "Shyam", 1, "Cloud", "HYD"),
      Emp(3, "Revanth", 2, "Bigdata", "BNGL"),
      Emp(4, "Raghu", 2, "Bigdata", "HYD"),
      Emp(6, "Apporva", 3, "Apac", "BNGL"),
      Emp(5, "Naga", 3, "Apac", "HYD"))

    val empDF = session.createDataFrame(emps)

    //Partitioning the data by deptName
    empDF.write.partitionBy("deptName").csv("output/test")

    //Partitioning the data by deptName,location
    empDF.write.partitionBy("deptName", "location").csv("output/test1")
    
    println("Done ....")
  }
}