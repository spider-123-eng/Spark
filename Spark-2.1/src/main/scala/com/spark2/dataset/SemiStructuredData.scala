package com.spark2.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object SemiStructuredData {

  case class University(name: String, numStudents: Long, yearFounded: Long)
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("SemiStructuredData").master("local[1]").getOrCreate()

    import session.implicits._
    val sc = session.sparkContext
    val sqlContext = new SQLContext(sc)

    val schools = sqlContext.read.json("input/schools.json").as[University]
    schools.printSchema()
    val res = schools.map(s => s"${s.name} is  ${2017 - s.yearFounded} years old")
    res.foreach { x => println(x) }
  }
}