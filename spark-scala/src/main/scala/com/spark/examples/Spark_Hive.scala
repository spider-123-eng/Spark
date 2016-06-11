package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
object Spark_Hive {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark_Hive").setMaster("local[1]")
    val sc = new SparkContext(conf)

    //create hive context
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //Create Table and load data 
    hiveContext.sql("CREATE TABLE IF NOT EXISTS users(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/hdp/dev/hive/users.txt' INTO TABLE users") //specify path to file accordingly
    
    val result = hiveContext.sql("FROM users SELECT id, name, age").show()

    val rddFromSql = hiveContext.sql("SELECT id,name,age FROM users WHERE age > 25 ORDER BY age")
    rddFromSql.show()

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    println("Result of RDD.map:")
    val rddAsStrings = rddFromSql.rdd.map {
      case Row(id: Int, name: String, age: Int) => s"Id: $id, Name: $name, Age: $age"
    }
    rddAsStrings.foreach { x => println(x) }

    // Aggregation queries are also supported.
    val count = hiveContext.sql("SELECT COUNT(*) FROM users").collect().head.getLong(0)
    println(s"count is : $count")

    // Queries are expressed in HiveQL
    println("Result of 'SELECT *': ")
    hiveContext.sql("SELECT * FROM users").collect().foreach(println)

  }
}