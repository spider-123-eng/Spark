package com.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector.toSparkContextFunctions
object Cassandra_SparkSQL {
  case class Emp(Id: Int, name: String, salary: String)
  def main(args: Array[String]) {
    //http://rustyrazorblade.com/2015/01/introduction-to-spark-cassandra/

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setAppName("Cassandra_SparkQL").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    try {
      val rows = sc.cassandraTable[Emp]("spark_kafka_cassandra", "employee")
      println("rows:" + rows.first())
      rows.toArray.foreach(println)

      val row = sc.cassandraTable[Emp]("spark_kafka_cassandra", "employee").select("id", "name", "salary").where("id = ?", "100")
      if (!row.isEmpty()) {
        println("row:" + row.first())
      } else {
        println("Records does not exist !")
      }

    } catch {
      case e: Exception =>
        println(e)
    }

  }
}
