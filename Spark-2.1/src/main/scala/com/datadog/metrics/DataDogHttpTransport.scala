package com.datadog.metrics

import org.apache.http.HttpHost
import org.apache.http.HttpResponse
import org.apache.http.client.fluent.Request.Post
import org.apache.http.client.fluent.Response
import org.apache.http.entity.ContentType
import org.apache.log4j.Logger

/**
 * Uses the datadog http webservice to push metrics.
 *
 * @see <a href="http://docs.datadoghq.com/api/">API docs</a>
 */

class DataDogHttpTransport(apiKey: String,
  private val connectTimeout: Int,
  private val socketTimeout: Int,
  private val proxy: HttpHost,
  isMetrics: java.lang.Boolean)
    extends Transport {

  val logger: Logger = Logger.getLogger(classOf[DataDogHttpTransport])

  private val BASE_URL: String = "https://app.datadoghq.com/api/v1"

  /**
   * seriesUrl gets constructed based on the 'isMetrics' value
   */
  private val seriesUrl: String =
    if (isMetrics) String.format("%s/series?api_key=%s", BASE_URL, apiKey)
    else String.format("%s/events?api_key=%s", BASE_URL, apiKey)

  /**
   * This method is used to send Metrics/Events to DataDog.
   * @return httpResponseCode
   */
  def sendToDataDog(transport: DataDogHttpTransport, jsonData: String): Int = {
    val request: org.apache.http.client.fluent.Request =
      Post(transport.seriesUrl)
        .useExpectContinue()
        .connectTimeout(transport.connectTimeout)
        .socketTimeout(transport.socketTimeout)
        .bodyString(jsonData, ContentType.APPLICATION_JSON)
    if (transport.proxy != null) {
      request.viaProxy(transport.proxy)
    }
    val response: Response = request.execute()
    val httpResponse: HttpResponse = response.returnResponse()
    httpResponse.getStatusLine.getStatusCode
  }

  /**
   * This method is used to send the Json request.
   * @return httpResponseCode
   */
  def send(jsonData: String) = sendToDataDog(this, jsonData)

  def close(): Unit = {}
}
