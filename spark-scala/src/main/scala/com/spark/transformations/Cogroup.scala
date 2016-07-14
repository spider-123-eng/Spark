package com.spark.transformations
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object Cogroup {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Transformations-Example").setMaster("local[1]")
    val sc = new SparkContext(conf)

    //  cartesian

    /*Computes the cartesian product between two RDDs (i.e. Each item of the first RDD is joined with each item of the second RDD)
  	and returns them as a new RDD. (Warning: Be careful when using this function.! Memory consumption can quickly become an issue!)
  	*/
    val x = sc.parallelize(List(1, 2, 3, 4, 5))
    val y = sc.parallelize(List(6, 7, 8, 9, 10))
    x.cartesian(y).collect.foreach(f => println(f))

    //cogroup
    println("cogroup ---cogroup----cogroup")
    val a = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 2)
    val b = sc.parallelize(List((1, "apple"), (5, "computer"), (1, "laptop"), (1, "desktop"), (4, "iPad")), 2)

    a.cogroup(b).collect.foreach(f => println(f))

    //subtract 2 RRD's
    val diff = a.subtract(b)
    diff.collect().foreach(f => println(f._2))

    //collectAsMap
    println("collectAsMap ---collectAsMap----collectAsMap")
    val c = sc.parallelize(List(1, 2, 1, 3), 1)
    val c2 = sc.parallelize(List(5, 6, 5, 7), 1)
    val d = c.zip(c2)
    d.collectAsMap.foreach(f => println(f))

    //combineByKey
    println("combineByKey ---combineByKey----combineByKey")
    val a1 = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val b1 = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val c1 = b1.zip(a1)
    val d1 = c1.combineByKey(List(_), (x: List[String], y: String) => y :: x, (x: List[String], y: List[String]) => x ::: y)
    d1.collect.foreach(f => println(f))

    //filterByRange [Ordered]
    println("filterByRange ---filterByRange----filterByRange")
    val randRDD = sc.parallelize(List((2, "cat"), (6, "mouse"), (7, "cup"), (3, "book"), (4, "tv"), (1, "screen"), (5, "heater")), 3)
    val sortedRDD = randRDD.sortByKey()

    sortedRDD.filterByRange(1, 3).collect.foreach(f => println(f))
  }
}