package com.spark2.elasticsearch

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
object Read_From_ES {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Spark-Read-ElasticSearch").master("local[*]").getOrCreate()

    val esConfig = Map(("es.nodes", "localhost"), ("es.port", "9200"),
      ("es.index.auto.create", "true"), ("es.http.timeout", "5m"),
      ("es.nodes.wan.only" -> "true"))

    // load the elasticsearch index into spark dataframe
    val df = spark.read.format("org.elasticsearch.spark.sql").load("blabla/joke")

    df.show(10, false)

  }

  //https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html
}