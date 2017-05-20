package com.spark2.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object Save_To_Hive {

  case class Purchase(customer_id: Int, purchase_id: Int, day: String, time: String, tz: String, amount: Double)

  def main(args: Array[String]) {

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val spark = SparkSession.builder.
      master("local[2]")
      .appName("Save_Saprk_To_Hive-Example")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config(" hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val purchaseDF = List(
      Purchase(121, 234, "2017-04-19", "20:50", "UTC", 500.99),
      Purchase(122, 247, "2017-04-19", "15:30", "PST", 300.22),
      Purchase(123, 254, "2017-04-19", "00:50", "EST", 122.19),
      Purchase(124, 234, "2017-04-19", "20:50", "UTC", 500.99),
      Purchase(125, 247, "2017-04-19", "15:30", "PST", 300.22),
      Purchase(126, 254, "2017-04-19", "00:50", "EST", 122.19),
      Purchase(125, 250, "2017-04-19", "15:30", "PST", 300.22),
      Purchase(126, 251, "2017-04-19", "00:50", "EST", 122.19),
      Purchase(127, 299, "2017-04-19", "07:30", "UTC", 524.37)).toDF()

    //Storing in to hive internal/managed tables
    purchaseDF.coalesce(1).write.mode(SaveMode.Append).insertInto("sales")

    sql("SELECT * FROM sales").show()

    //Storing in to hive external tables
    purchaseDF.coalesce(1).write.mode(SaveMode.Append).insertInto("sales_ext")

    sql("SELECT * FROM sales_ext").show()

  }

  /*  CREATE TABLE IF NOT EXISTS sales ( customer_id int, purchase_id int,day String, time String, tz String, amount double)
COMMENT 'Sales Data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE  LOCATION '/user/centos/hive/sale';
*/

  /*  
CREATE EXTERNAL TABLE IF NOT EXISTS sales_ext ( customer_id int, purchase_id int,day String, time String, tz String, amount double)
COMMENT 'Sales Data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE  LOCATION '/user/centos/hive/sale_ext';
*/

}