package com.spark.transformations.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.spark.util._
object Cars {
  def main(args: Array[String]) {
    case class cars(make: String, model: String, mpg: String, cylinders: Integer, engine_disp: Integer, horsepower: Integer, weight: Integer, accelerate: Double, year: Integer, origin: String)
    val conf = new SparkConf().setAppName("Transformations").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile(Utills.DATA_PATH +"cars.txt") //"path to/cars.txt"
    
    rawData.take(5).foreach { x => println(x) }

    val carsData = rawData.map(x => x.split("\t"))
      .map(x => cars(x(0).toString, x(1).toString, x(2).toString, x(3).toInt, x(4).toInt, x(5).toInt, x(6).toInt, x(7).toDouble, x(8).toInt, x(9).toString))

    carsData.take(2).foreach { x => println(x) }
    //persist to memory
    carsData.cache()

    //count cars origin wise
    val originWiseCount = carsData.map(x => (x.origin, 1)).reduceByKey((x, y) => x + y)
    println("originWiseCount :" + originWiseCount.collect().mkString(", "))
    //filter out american cars
    val americanCars = carsData.filter(x => (x.origin == "American"))

    //count total american cars
    println("americanCars count : " + americanCars.count())

    // take sum of weights according to make
    val makeWeightSum = americanCars.map(x => (x.make, x.weight.toInt)).combineByKey((x: Int) => (x, 1),
      (acc: (Int, Int), x) => (acc._1 + x, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    println("americanCars makeWeightSum : " + makeWeightSum.collect().mkString(", "))

    // take average
    val makeWeightAvg = makeWeightSum.map(x => (x._1, (x._2._1 / x._2._2)))
    
    
    println("americanCars makeWeightAvg : " +makeWeightAvg.collect().mkString(", "))
  }
}