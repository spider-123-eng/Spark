package com.spark.cassandra
import org.apache.spark.{ SparkContext, SparkConf }
import com.datastax.spark.connector._
import org.apache.spark._
import java.util.UUID
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql._
object CassandraCQL extends App {

  case class Emp(Id: Int, name: String, salary: String)

  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setAppName("CassandraCQL").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  //implementation using cassandra sql context
  val cc = new CassandraSQLContext(sc)
  val rdd = cc.sql("SELECT id,name,salary FROM spark_kafka_cassandra.employee where name like 'HARI%'")

  rdd.collect().foreach(println)

 /* //implementation using cassandra table converting to df
  val user_table = sc.cassandraTable("tutorial", "user")

  val df = sqlContext
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "user", "keyspace" -> "tutorial"))
    .load()

  df.registerTempTable("user")
  val results = sqlContext.sql("SELECT empname,sum(empsal),sum(projno) FROM tutorial.user GROUP BY empid,empname,deptno")
  //results.collect().foreach(println)
*/  sc.stop
}