package com.spark2.hive

import org.apache.spark.sql.SparkSession

object Spark_CatalogAPI {

  def main(args: Array[String]) {

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val spark = SparkSession.builder.
      master("local[2]")
      .appName("Spark-Catalog-Example")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()

    //interacting with catalogue

    val catalog = spark.catalog

    //print the databases

    catalog.listDatabases().foreach { x => println(x) }
    catalog.setCurrentDatabase("default")
    catalog.listTables.show
    catalog.listColumns("employee").foreach { x => println(x) }

    import spark.implicits._
    import spark.sql

    sql("SELECT * FROM employee").show()
  }

}