package com.spark2.examples
import org.apache.spark.sql.SparkSession

object Spark_To_ObjectFile {

  case class Emp(empId: Int, emp_name: String, deptId: Int, deptName: String, location: String)

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("Spark_To_ObjectFile").master("local[1]").getOrCreate()
    val sc = session.sparkContext

    val nums = sc.makeRDD(1 to 3).map(x => (x, "a" * x))
    //nums.saveAsObjectFile("output/test")

    // Try reading the output back as an object file
    val output = sc.objectFile[(Int, String)]("output/test")
    output.foreach(f => println(f))

    val emps = List(
      Emp(1, "Mike", 1, "Cloud", "BNGL"),
      Emp(2, "Shyam", 1, "Cloud", "HYD"),
      Emp(3, "Revanth", 2, "Bigdata", "BNGL"),
      Emp(4, "Raghu", 2, "Bigdata", "HYD"),
      Emp(6, "Apporva", 3, "Apac", "BNGL"),
      Emp(5, "Naga", 3, "Apac", "HYD"))

    //Saving rdd as ObjectFile and reading back
    val empRDD = sc.parallelize(emps)
    empRDD.saveAsObjectFile("output/empRdd")

    val resRDD = sc.objectFile[Any]("output/empRdd")
    resRDD.foreach(f => println(f))

    //Saving DataFrame as ObjectFile and reading back
    import session.implicits._
    val empDF = emps.toDF()
    empDF.rdd.saveAsObjectFile("output/empDF")

    val resDF = sc.objectFile[Any]("output/empDF")
    resDF.foreach(f => println(f))

  }
}