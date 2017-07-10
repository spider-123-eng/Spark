package com.spark2.problemstatements

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object ProblemStatement {

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-ProblemStatement")
        .master("local[2]")
        .getOrCreate()
    import spark.implicits._

    //What are the best-salary and the second best-salary of employees in every department?

    val dataRDD = spark.read.format("csv").option("header", "true").load("input/pbs.csv").rdd
    val filteredDF = dataRDD.map(x => (x(2).toString(), x(3).toString().replace("$", "").toDouble)).toDF("dept", "salary").dropDuplicates().toDF()

    val maxSalDF = filteredDF.groupBy("dept").agg(max(filteredDF.col("salary")).as("MaxSal")).sort("dept")
    maxSalDF.show

    val subDF = filteredDF.except(maxSalDF)

    val ScndMaxSalDF = subDF.groupBy("dept").agg(max(subDF.col("salary")).as("SecMaxSal")).sort("dept")
    ScndMaxSalDF.show

    val pboblem1ResDF = maxSalDF.join(ScndMaxSalDF, Seq("dept")).sort("dept").toDF()
    pboblem1ResDF.show
    pboblem1ResDF.coalesce(1).write.option("header", "true").csv("/Users/revanthreddy/Desktop/Docs/file1.csv")

    //What is the difference between the salary of each employee and the highest salary of employee in the same department?

    val pboblem2DF = dataRDD.map(x => (x(0).toString(), x(2).toString(), x(3).toString().replace("$", "").toDouble)).toDF("name", "dept", "salary").dropDuplicates().toDF()

    val resDF = pboblem2DF.join(maxSalDF, Seq("dept")).sort("dept").toDF()

    val pboblem2ResDF = resDF.withColumn("diffSal", (resDF.col("MaxSal") - resDF.col("salary")))
    pboblem2ResDF.coalesce(1).write.option("header", "true").csv("/Users/revanthreddy/Desktop/Docs/file2.csv")

  }
}