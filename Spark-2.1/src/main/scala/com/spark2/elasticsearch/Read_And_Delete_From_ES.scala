package com.spark2.elasticsearch

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.apache.spark.sql.functions._
import java.net.InetAddress
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper
import org.elasticsearch.index.query.QueryParsingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.JsonParser

object Read_And_Delete_From_ES {

  var client: TransportClient = _

  def deleteByQuery(): DeleteByQueryRequestBuilder = {
    return DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
  }

  def main(args: Array[String]) {
    val clusterName = args(0)
    val hostNames = args(1)
    val index = "device_profile/station"

    val settings = Settings.builder()
      .put("cluster.name", clusterName)
      .put("client.transport.sniff", true)
      .put("client.transport.ignore_cluster_name", true).build()

    val esPort = 9300
    val docType = "station"

    client = hostNames.split(",").foldLeft(TransportClient.builder().settings(settings)
      .addPlugin(classOf[DeleteByQueryPlugin])
      .build()) {
      case (builder, host) =>
        builder.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), esPort))
    }

    println(s"ES Object initialized. ES Object (${client}) ")

    val spark = SparkSession.builder().appName("Spark-Read-ElasticSearch").getOrCreate()

    val esConfig = Map(("es.nodes", "172.16.60.36"), ("es.port", "9200"), ("es.index.auto.create", "false"), ("es.http.timeout", "5m"))

    val readEsIndex = spark.read.format("org.elasticsearch.spark.sql").options(esConfig).load(index)

    val sdf = readEsIndex.select("tid", "sta_mac", "user_name").where("tid == 139")

    val sdf1 = sdf.select("tid", "sta_mac", "user_name").where(length(col("user_name")) < 64)
    val mapper = new ObjectMapper()
    mapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true)
    sdf1.collect().foreach { f =>
      try {
        val tid = f(0).toString()
        val stamac = f(1).toString()
        val id = "\"" + (tid + stamac) + "\""

        println("Record marked for deletion is : " + id)

        val b = new StringBuilder
        b.append("{")
        b.append("\"query\": {")
        b.append("\"term\": {")
        b.append("\"_id\": " + id)
        b.append("}")
        b.append("}")
        b.append("}")

        val json = mapper.readTree(b.toString()).toString()

        println("<----------------- delete query -----------------------> " + b.toString())

        val response = deleteByQuery.setIndices("device_profile")
          .setTypes(docType)
          .setSource(json)
          .execute()
          .actionGet()

        Thread.sleep(1000)
        println("Is record successfully deleted : " + id)

      } catch {
        case _: NotSerializableExceptionWrapper | _: QueryParsingException =>
          println("Error while deleting the ES documents ")

      }
    }

    //    sdf1.printSchema()
    //
    //    sdf1.show(10, false)

  }

  //spark-shell --jars elasticsearch-spark_2.11-2.2.0.jar 
}