package com.spark.transformations.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object GroupBY_ReduceBY {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-GroupBY-ReduceBY-Example").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val words = Array("a", "b", "b", "c", "d", "e", "a", "b", "b", "c", "d", "e", "b", "b", "c", "d", "e")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD
      .reduceByKey(_ + _)
      .collect()
    wordCountsWithReduce.foreach(f => println(f))

    //Avoid GroupByKey
    println("Avoid GroupByKey")
    val wordCountsWithGroup = wordPairsRDD
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect()
    wordCountsWithGroup.foreach(f => println(f))
  }

  //https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
}