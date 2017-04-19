package com.spark.customudf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CustomUDF {
  val sparkConf = new SparkConf().setAppName("Spark-CustomUDF").setMaster("local[1]")//.set("spark.sql.warehouse.dir", "file:///D:/Spark-WorkSpace/Spark-Windows/spark-warehouse")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  def main(args: Array[String]) {

    // Construct Dummy Data
    import util.Random
    import org.apache.spark.sql.Row
    implicit class Crossable[X](xs: Traversable[X]) {
      def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
    }

    val students = Seq("John", "Mike", "Matt")
    val subjects = Seq("Math", "Sci", "Geography", "History")
    val random = new Random(1)
    val data = (students cross subjects).map { x => Row(x._1, x._2, random.nextInt(100)) }.toSeq

    data.foreach { x => println(x)}
    
    // Create Schema Object
    import org.apache.spark.sql.types.{ StructType, StructField, IntegerType, StringType }
    val schema = StructType(Array(
      StructField("student", StringType, nullable = false),
      StructField("subject", StringType, nullable = false),
      StructField("score", IntegerType, nullable = false)))

    // Create DataFrame 
    val rdd = sc.parallelize(data)
    val df = sqlContext.createDataFrame(rdd, schema)
    // Define udf
    import org.apache.spark.sql.functions.udf
    def udfScoreToCategory = udf((score: Int) => {
      score match {
        case t if t >= 80 => "A"
        case t if t >= 60 => "B"
        case t if t >= 35 => "C"
        case _            => "D"
      }
    })
    df.withColumn("category", udfScoreToCategory(df("score"))).show(10)
  }

}