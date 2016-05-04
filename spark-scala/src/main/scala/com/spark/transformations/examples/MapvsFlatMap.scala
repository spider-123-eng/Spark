package com.spark.transformations.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object MapvsFlatMap {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MapvsFlatMap").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val x = sc.parallelize(List("spark rdd example", "sample example"))

    // map operation will return Array of Arrays in following case : check type of result
    val y = x.map(x => x.split(" ")) // split(" ") returns an array of words
    //result ->  Array[Array[String]] = Array(Array(spark, rdd, example), Array(sample, example))

    /*Similar to map, but each input item can be mapped to 0 or more output items 
    (so func should return a Seq rather than a single item).*/

    // flatMap operation will return Array of words in following case : Check type of result
    val z = x.flatMap(x => x.split(" "))
    z.collect().foreach { x => println(x) }
    //result -> Array[String] = Array(spark, rdd, example, sample, example)

  }
}