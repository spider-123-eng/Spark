package com.spark.cassandra

import scala.reflect.runtime.universe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toSparkContextFunctions
object Spark_Cassandra {
  case class Employee(Id: Int, name: String, salary: Int)
  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setAppName("Spark_Cassandra").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val KEY_SPACE_NAME = "spark_kafka_cassandra"
    val TABLE_NAME = "employee"
    try {

      //Saving the records
      val save = sc.parallelize(Seq(Employee(123, "Shyam Sunder", 12345), Employee(345, "Prabhakar Reddy", 50000)))
      save.map(emp => (emp.Id, emp.name, emp.salary)).saveToCassandra(KEY_SPACE_NAME, TABLE_NAME, SomeColumns(
        "id", "name", "salary"))

      //Updating the records
      val upate = sc.parallelize(Seq(Employee(123, "Shyam Sunder Reddy", 333444), Employee(328565, "Hari", 9999999)))
      upate.map(emp => (emp.Id, emp.name, emp.salary)).saveToCassandra(KEY_SPACE_NAME, TABLE_NAME)

      //Displaying the records
      val rows = sc.cassandraTable[Employee](KEY_SPACE_NAME, TABLE_NAME).cache()
      rows.toArray.foreach(println)

      //Column selection
      sc.cassandraTable(KEY_SPACE_NAME, TABLE_NAME).select("id", "name").take(10).foreach(println)

      //Using Where clause  
      val row = sc.cassandraTable[Employee](KEY_SPACE_NAME, TABLE_NAME).select("id", "name", "salary").where("id = ?", "100")
      if (!row.isEmpty()) {
        row.collect().foreach { x => println(x) }
      } else {
        println("Records does not exist !")
      }

      //Using groupBy
      rows.groupBy(x => x.name).foreach(f => println(f))

      rows.map(row => row.name.toUpperCase()).foreach { x => println(x) }

      //Using soryBy
      rows.sortBy(f => f.Id, true, 3).foreach { x => println(x) }

      //performing Aggregations
      println("Sum of Salaries => " + rows.map(row => row.salary).sum)

      println("Max Salary => " + rows.map(row => row.salary).max())

    } catch {
      case e: Exception =>
        println(e)
    }

  }

  /* cqlsh> CREATE KEYSPACE IF NOT EXISTS spark_kafka_cassandra WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    cqlsh> use spark_kafka_cassandra ;
    cqlsh:spark_kafka_cassandra> CREATE TABLE IF NOT EXISTS spark_kafka_cassandra.employee (id int PRIMARY KEY,name VARCHAR, salary int);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (268988,'venkat', 100);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (328561,'revanth', 200);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (328562,'reddy', 200);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (328563,'Ravireddy', 200);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (328564,'RaviKanth', 200);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (328565,'Hari', 200);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (100,'HariPrasad', 200);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (101,'Prasad', 2000);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (102,'SivaPrasad', 2000);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (103,'Siva', 20000);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (103,'VeersSiva', 20000);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (103,'Siva', 20000);
    cqlsh:spark_kafka_cassandra> INSERT INTO employee(id,name, salary) VALUES (104,'VeersSiva', 20000);
    cqlsh:spark_kafka_cassandra> select * from employee ; */
}