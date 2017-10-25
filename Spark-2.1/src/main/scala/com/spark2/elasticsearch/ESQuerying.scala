package com.spark2.elasticsearch

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
object ESQuerying {
  case class Purchase(customer_id: Int, purchase_id: Int, date: String, time: String, tz: String, amount: Double)
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Spark-ElasticSearch-Querying").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val esConfig = Map(("es.nodes", "localhost"), ("es.port", "9200"),
      ("es.index.auto.create", "true"), ("es.http.timeout", "5m"),
      ("es.nodes.wan.only" -> "true"))

    val index = "sales/data"

    val purchaseDF = List(
      Purchase(121, 234, "2017-09-26 05:00:00.0", "20:50", "UTC", 500.99),
      Purchase(122, 247, "2017-07-26 05:00:00.0", "15:30", "PST", 300.22),
      Purchase(123, 254, "2017-09-26 05:00:00.0", "00:50", "EST", 122.19),
      Purchase(124, 234, "2017-09-26 04:00:00.0", "20:50", "UTC", 500.99),
      Purchase(125, 247, "2017-08-26 05:00:00.0", "15:30", "PST", 300.22),
      Purchase(126, 254, "2017-08-26 05:00:00.0", "00:50", "EST", 122.19),
      Purchase(127, 250, "2017-08-26 05:00:00.0", "15:30", "PST", 300.22),
      Purchase(128, 251, "2017-07-26 07:00:00.0", "00:50", "EST", 122.19),
      Purchase(129, 299, "2017-07-26 05:00:00.0", "07:30", "UTC", 524.37)).toDF()

    //writing to ElasticSearch index
    purchaseDF.saveToEs(index, cfg = esConfig)

    // load the elasticsearch index into spark dataframe
    val df = spark.read.format("org.elasticsearch.spark.sql").load(index)
    df.printSchema()
    val filtDF = df.filter(df.col("customer_id").equalTo("121").and(df.col("purchase_id").equalTo("234")))
    filtDF.show(false)

    //if you want to load only specific fields
    val esreadFieldsConfig = Map(("es.nodes", "localhost"), ("es.port", "9200"),
      ("es.index.auto.create", "true"), ("es.http.timeout", "5m"),
      ("es.nodes.wan.only" -> "true"), ("es.read.field.include" -> "customer_id,purchase_id,amount"))

    val salesDF = spark.esDF(index, esreadFieldsConfig)
    salesDF.show(5, false)

    //querying the ElasticSearch index using customer_id as DF
    val salesQueryDF = spark.esDF(index, "?q=customer_id:123", esreadFieldsConfig)
    salesQueryDF.show(false)

    //querying the ElasticSearch index using customer_id as RDD
    val rdd = sc.esRDD(index, "?q=customer_id:123", esConfig)

    val customer_rdd = rdd.map {
      case (docId, doc) => (docId, doc.get("customer_id").get.asInstanceOf[Long], doc.get("purchase_id").get.asInstanceOf[Long],
        doc.get("amount").get.asInstanceOf[Float], doc.get("time").get.asInstanceOf[String])
    }
    customer_rdd.take(10).foreach(println)

  }
}