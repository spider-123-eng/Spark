package com.spark2.elasticsearch

import java.net.InetAddress

import org.elasticsearch.action.deletebyquery.DeleteByQueryAction
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin
import org.slf4j.LoggerFactory
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.common.xcontent.XContentFactory._
import java.util.Date

object ESDeleteByQuery extends App {
  protected val logger = LoggerFactory.getLogger(getClass)
  val esIndex = "twitter"
  val esType = "tweet"
  var client: TransportClient = _
  val clusterName = "dev"
  val esNode = "localhost"
  val tid = 100

  def deleteByQuery(): DeleteByQueryRequestBuilder = {
    return DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
  }

  try {

    val settings = Settings.settingsBuilder()
      .put("cluster.name", clusterName)
      .put("client.transport.sniff", true)
      .build()

    this.client = TransportClient.builder().settings(settings)
      .addPlugin(classOf[DeleteByQueryPlugin])
      .build()
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esNode), 9300))

    logger.info("List of connected ES nodes : " + client.connectedNodes())

    var docId = 0
    for (docId <- 1 to 10) {
      val insert = client.prepareIndex("twitter", "tweet", docId.toString())
        .setSource(jsonBuilder()
          .startObject()
          .field("user", "kimchy " + docId)
          .field("tid", "100")
          .field("postDate", new Date())
          .field("message", "trying out Elasticsearch " + docId)
          .endObject())
        .get()
    }

    Thread.sleep(1000)
    //    val response = client.prepareGet(esIndex, esType, "2").get()
    //    println("Is documentd found : "+response.isExists())
    //
    //    for (docId <- 1 to 10) {
    //      val delResponse = client.prepareDelete(esIndex, esType, docId.toString).get()
    //    }
    //    //Thread.sleep(1000)
    //
    val documentsCount = client.prepareCount(esIndex)
      .setQuery(termQuery("_type", esType)).execute().actionGet()
    logger.info(s"Total documents count for index '$esIndex/$esType' are : " + documentsCount.getCount)

    val b = new StringBuilder
    b.append("{")
    b.append("\"query\": {")
    b.append("\"term\": {")
    b.append("\"tid\": " + 100)
    b.append("}")
    b.append("}")
    b.append("}")

    val response = deleteByQuery.setIndices(esIndex)
      .setTypes(esType)
      .setSource(b.toString())
      .execute()
      .actionGet()

    Thread.sleep(1000)
    logger.info(s"Total documents found for index '$esIndex/$esType' is " + response.getTotalFound)
    logger.info(s"Total documents deleted for index '$esIndex/$esType' is " + response.getTotalDeleted)
    logger.info(s"Total documents failed for deletion for index  '$esIndex/$esType' is " + response.getTotalFailed)

  } catch {
    case ex: Exception =>
      logger.error("Error while initializing TransportClient :" + ex.getMessage(), ex)
      throw new RuntimeException("Error while initialize TransportClient. Failing job")
  }
}