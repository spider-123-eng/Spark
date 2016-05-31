package com.spark.examples

import scala.reflect.runtime.universe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.toSparkContextFunctions
import org.apache.log4j.Logger
import org.apache.log4j.Level
object Spark_Cassandra_Delete {
  case class Employee(Id: Int, name: String, salary: Int)
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val CASSANDRA_HOST = "127.0.0.1"
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", CASSANDRA_HOST).setAppName("Spark-Cassandra-Delete").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val KEY_SPACE_NAME = "spark_kafka_cassandra"
    val TABLE_NAME = "employee"

    //Creating Cluster object
    val cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).build()
    //Creating Session object
    val session = cluster.connect()

    try {
      val deleteQuery = " delete from " + KEY_SPACE_NAME + "." + TABLE_NAME + " WHERE id =  102 "
      val deletequeryprepared = session.prepare(deleteQuery)
      val deleteBoundStatement = new BoundStatement(deletequeryprepared)
      session.execute(deleteBoundStatement)

      //Displaying the records
      val rows = sc.cassandraTable[Employee](KEY_SPACE_NAME, TABLE_NAME)
      rows.toArray.foreach(println)

    } catch {
      case e: Exception =>
        println(e)
    } finally {
      session.close()
      cluster.close()
      sc.stop()
    }

  }
}
