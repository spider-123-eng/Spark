package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
object Spark_Hive_ORC {
  case class YahooStockPrice(date: String, open: Float, high: Float, low: Float, close: Float, volume: Integer, adjClose: Float)
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark_Hive_ORC").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //create hive context
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //Create ORC Table and load data 
    hiveContext.sql("create EXTERNAL table yahoo_orc_table (date STRING, open_price FLOAT, high_price FLOAT, low_price FLOAT, close_price FLOAT, volume INT, adj_price FLOAT) stored as orc")

    val yahoo_stocks = sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/data/yahoo_stocks.csv")

    //filter out the header information
    val header = yahoo_stocks.first
    val data = yahoo_stocks.filter(_ != header)

    //Map the data to a case class and register it as a temp table.
    val stockprice = data.map(_.split(",")).map(row => YahooStockPrice(row(0), row(1).trim.toFloat, row(2).trim.toFloat, row(3).trim.toFloat, row(4).trim.toFloat, row(5).trim.toInt, row(6).trim.toFloat)).toDF()
    stockprice.registerTempTable("yahoo_stocks_temp")
    val results = sqlContext.sql("SELECT * FROM yahoo_stocks_temp")

    results.map(t => "Stock Entry: " + t.toString).collect().foreach(println)

    //save the data to HDFS in ORC file format.
    results.coalesce(1).write.format("orc").save("/user/data/yahoo_stocks_orc")

    //load the data in ORC format to visualize it .
    val yahoo_stocks_orc = hiveContext.read.format("orc").load("/user/data/yahoo_stocks_orc")
    yahoo_stocks_orc.registerTempTable("orcTest")
    hiveContext.sql("SELECT * from orcTest").collect.foreach(println)

    //load the ORC data in to ORC hive table created at the top. 
    hiveContext.sql("LOAD DATA INPATH '/user/data/yahoo_stocks_orc' INTO TABLE yahoo_orc_table")
    val orcResults = hiveContext.sql("FROM yahoo_orc_table SELECT date, open_price,high_price")
    orcResults.show

  }
}