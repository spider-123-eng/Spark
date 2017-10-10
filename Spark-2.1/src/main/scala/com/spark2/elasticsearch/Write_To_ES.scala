package com.spark2.elasticsearch

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._
object Write_To_ES {
  case class SimpsonCharacter(name: String, actor: String, episodeDebut: String)

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Spark-Write-ElasticSearch").master("local[*]").getOrCreate()

    val index = "shows/data"

    val esConfig = Map(("es.nodes", "localhost"), ("es.port", "9200"),
      ("es.index.auto.create", "true"), ("es.http.timeout", "5m"),
      ("es.nodes.wan.only" -> "true"))

    import spark.implicits._

    val simpsonsDF = spark.sparkContext.parallelize(
      SimpsonCharacter("Homer", "Dan Castellaneta", "Good Night") ::
        SimpsonCharacter("Marge", "Julie Kavner", "Good Night") ::
        SimpsonCharacter("Bart", "Nancy Cartwright", "Good Night") ::
        SimpsonCharacter("Lisa", "Yeardley Smith", "Good Night") ::
        SimpsonCharacter("Maggie", "Liz Georges and more", "Good Night") ::
        SimpsonCharacter("Sideshow Bob", "Kelsey Grammer", "The Telltale Head") ::
        Nil).toDF().repartition(1)

    //writing to ElasticSearch index
    simpsonsDF.saveToEs(index, cfg = esConfig)

    //reading from ElasticSearch index
    val df = spark.read.format("org.elasticsearch.spark.sql").load(index)
    df.show(10, false)
  }
  //https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html
  //https://marekj.github.io/2016/03/22/elasticsearch-mac-osx
}