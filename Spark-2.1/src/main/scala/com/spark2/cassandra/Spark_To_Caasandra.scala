package com.spark2.cassandra

import scala.reflect.runtime.universe

import org.apache.spark.sql.SparkSession

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toSparkContextFunctions
import org.apache.spark.sql.SaveMode

object Spark_To_Caasandra extends App {

  case class Employee(id: Int, name: String, salary: Int)

  val spark = SparkSession.builder().appName("Spark_To_Caasandra").master("local[1]").getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "2")
  spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")

  val KEY_SPACE_NAME = "dev"
  val TABLE_NAME = "employee"

  //loading data from cassandra table
  val df = spark.read.format("org.apache.spark.sql.cassandra").option("table", TABLE_NAME)
    .option("keyspace", KEY_SPACE_NAME)
    .load()
  df.printSchema()
  df.show()

  val sc = spark.sparkContext
  import spark.implicits._

  //Write Spark Dataframe to cassandra
  val empDF = Seq(Employee(123, "Sunder", 3030), Employee(345, "Revanth Reddy", 1010)).toDF()
  empDF.write.format("org.apache.spark.sql.cassandra").option("table", TABLE_NAME)
    .option("keyspace", KEY_SPACE_NAME).mode(SaveMode.Append).save()

  //Saving the records
  val save = sc.parallelize(Seq(Employee(123, "Shyam Sunder", 12345), Employee(345, "Hari", 50000)))
  save.map(emp => (emp.id, emp.name, emp.salary)).saveToCassandra(KEY_SPACE_NAME, TABLE_NAME, SomeColumns(
    "id", "name", "salary"))

  //Updating the records (salary is updated)
  val upate = sc.parallelize(Seq(Employee(123, "Shyam Sunder", 300), Employee(345, "Hari", 400)))
  upate.map(emp => (emp.id, emp.name, emp.salary)).saveToCassandra(KEY_SPACE_NAME, TABLE_NAME)

  //Displaying the records
  val empRows = sc.cassandraTable[Employee](KEY_SPACE_NAME, TABLE_NAME)
  empRows.foreach { x => println(x) }

  //Column selection
  val emps = spark.sparkContext.cassandraTable(KEY_SPACE_NAME, TABLE_NAME).select("id", "name")
  emps.foreach { x => println(x) }

  //Using Where clause  
  val row = sc.cassandraTable[Employee](KEY_SPACE_NAME, TABLE_NAME).select("id", "name", "salary").where("salary > ?", "200")
  if (!row.isEmpty()) {
    row.collect().foreach { x => println(x) }
  } else {
    println("Records does not exist !")
  }
  
  /* cqlsh> CREATE KEYSPACE IF NOT EXISTS dev WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    cqlsh> use dev ;
    cqlsh:spark_kafka_cassandra> CREATE TABLE IF NOT EXISTS dev.employee (id int PRIMARY KEY,name VARCHAR, salary int);
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
    cqlsh:spark_kafka_cassandra> select * from employee ;  */
}
