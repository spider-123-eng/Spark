package com.spark2.elasticsearch

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._

object CsvToESLoad {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("CsvToESLoad").master("local[*]").getOrCreate()

    val esConfig = Map(("es.nodes", "localhost"), ("es.port", "9200"),
      ("es.index.auto.create", "true"), ("es.http.timeout", "5m"),
      ("es.nodes.wan.only" -> "true"))

    val index = "realestatedata/data"

    import spark.implicits._

    val esdf = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("input/Real_Estate_Data.csv")

    esdf.show(2, false)

    //writing to ElasticSearch index
    esdf.saveToEs(index, cfg = esConfig)

    //reading from ElasticSearch index
    val df = spark.read.format("org.elasticsearch.spark.sql").load(index)
    df.show(10, false)
  }
  //https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html
}