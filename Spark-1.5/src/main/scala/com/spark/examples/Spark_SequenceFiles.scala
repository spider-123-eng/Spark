package com.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Spark_SequenceFiles {
  
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark-Sequence-Files").setMaster("local[1]"))

    val data = sc.textFile("file:////data/Spark/spark-scala/src/main/resources/olympics_data.txt")

    data.map(x => x.split(",")).map(x => (x(1).toString(), x(2).toString())).foreach(f => print(f))

    val pairs: RDD[(String, String)] = data.map(x => x.split(",")).map(x => (x(1).toString(), x(2).toString()))

    pairs.saveAsSequenceFile("/data/spark/rdd_to_seq")

    //Loading sequenceFiles into an RDD in Spark

    val data1: RDD[(String, String)] = sc.sequenceFile("/data/spark/rdd_to_seq")

    data1.take(5).foreach(f => print(f))
  }
}