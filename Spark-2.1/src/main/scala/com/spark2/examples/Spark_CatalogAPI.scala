package com.spark2.examples

import org.apache.spark.sql.SparkSession

object Spark_CatalogAPI {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local[2]")
      .appName("Spark-Catalog-Example")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "/Users/revanthreddy/Project/Spark-2.1/spark-warehouse")
      .getOrCreate()

    //interacting with catalogue

    val catalog = sparkSession.catalog

    //print the databases

    catalog.listDatabases().foreach { x => println(x) }
    catalog.setCurrentDatabase("airline_db")
    catalog.listTables.show
    catalog.listColumns("airline").foreach { x => println(x) }

    import sparkSession.implicits._
    import sparkSession.sql

    sql("SELECT * FROM airline limit 3").show()
  }

}