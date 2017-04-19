package com.spark.transformations
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object AggregateByKey {

  def myfunc(index: Int, iter: Iterator[(String, Int)]): Iterator[String] = {
    iter.toList.map(x => "[partID:" + index + ", val: " + x + "]").iterator
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AggregateByKey-Example").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val pairRDD = sc.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2)), 2)

    //lets have a look at what is in the partitions
    pairRDD.mapPartitionsWithIndex(myfunc).collect.foreach(f => println(f))
    println("***********************************************")
   
    pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect.foreach(f => println(f))
    println("-----------------------------------------------")

    pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect.foreach(f => println(f))
  }
}