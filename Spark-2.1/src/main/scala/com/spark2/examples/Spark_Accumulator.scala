package com.spark2.examples

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark_Accumulator {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("Spark_Accumulator")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val badPkts = sc.longAccumulator("bad.packets")
    val zeroValueSales = sc.longAccumulator("Zero.Value.Sales")
    val missingFields = sc.longAccumulator("Missing.Fields")
    val blankLines = sc.longAccumulator("Blank.Lines")

    sc.textFile("input/purchases.log", 4)
      .foreach { line =>

        if (line.length() == 0) blankLines.add(1)
        else if (line.contains("Bad data packet")) badPkts.add(1)
        else {
          val fields = line.split("\t")
          if (fields.length != 4) missingFields.add(1)
          else if (fields(3).toFloat == 0) zeroValueSales.add(1)
        }
      }

    println("Purchase Log Analysis Counters:")
    println(s"\tBad Data Packets=${badPkts.value}")
    println(s"\tZero Value Sales=${zeroValueSales.value}")
    println(s"\tMissing Fields=${missingFields.value}")
    println(s"\tBlank Lines=${blankLines.value}")

    sc.stop
  }
}