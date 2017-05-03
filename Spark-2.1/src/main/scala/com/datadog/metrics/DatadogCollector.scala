package com.datadog.metrics

import org.json4s.jackson.Serialization.write
import org.slf4j.LoggerFactory

import com.datadog.metrics.CaseClasses.Event
import com.datadog.metrics.CaseClasses.Series
import com.datadog.metrics.CaseClasses.SeriesList
/**
 * @author revanthreddy
 */
class DataDogCollector(apikey: String, env: String) extends MetricsCollector {

  protected val logger = LoggerFactory.getLogger(getClass)

  //initializing the DataDog HttpTransporter
  val metricsHttpTransport = new DataDogHttpTransport(apikey, 1, 1, null, true)
  val eventsHttpTransport = new DataDogHttpTransport(apikey, 5000, 5000, null, false)

  //max retry count if httpTransport connection fails
  val max_re_tyries = 3

  /**
   * This method is used to construct DataDog metrics.
   */
  def sendMetrics(metricName: String, metricValue: Long, metricsTags: collection.mutable.Map[String, Any]) = {
    try {
      var metricsTagsList: List[String] = List()
      var points: List[Long] = List()
      var finalpoints: List[List[Long]] = List()
      val time = System.currentTimeMillis() / 1000
      points = points.+:(metricValue)
      points = points.+:(time)

      if (metricsTags != null) {
        for ((key, value) <- metricsTags) {
          metricsTagsList = metricsTagsList.+:(key + ":" + value)
        }
      }
      metricsTagsList = metricsTagsList.+:("env" + ":" + env)
      finalpoints = finalpoints.+:(points)
      val seriesList = SeriesList(List(Series(metricName, "gauge", finalpoints, metricsTagsList)))

      //push the collected metrics to DataDog
      pushMetricsToDataDog(seriesList, true)
    } catch {
      case e: Exception =>
        logger.error("Error in DataDogCollector.sendMetrics()" + e.getMessage)
    }
  }

  /**
   * This method is used to construct DataDog events.
   */
  def sendEvents(title: String, text: String, priority: String, alert_type: String, eventTags: collection.mutable.Map[String, Any]) = {
    try {
      var eventTagsList: List[String] = List()
      if (eventTags != null) {
        for ((key, value) <- eventTags) {
          eventTagsList = eventTagsList.+:(key + ":" + value)
        }
      }
      eventTagsList = eventTagsList.+:("env" + ":" + env)
      val time = System.currentTimeMillis() / 1000

      val event = Event(title, text, getPriority(priority), getAlert_type(alert_type), time, eventTagsList)

      //push the collected events to DataDog
      pushEventsToDataDog(event, false)
    } catch {
      case e: Exception => logger.error("Error in DataDogCollector.sendEvents()" + e.getMessage)
    }
  }

  /**
   * This method is used to push metrics to the DataDog API.
   */
  def pushMetricsToDataDog(seriesList: SeriesList, isMetrics: Boolean) {
    val jsonOutString = generateJsonString(seriesList)
    try {
      metricsHttpTransport.send(jsonOutString)
      logger.info("Metrics Json : " + jsonOutString)

    } catch {
      case ex: org.apache.http.conn.HttpHostConnectException =>
        logger.error("Error in DataDogCollector.pushMetricsToDataDog()" + ex)
        httpTransportConnectionRetry(jsonOutString, metricsHttpTransport, max_re_tyries)

    }
  }

  /**
   * This method is used to push events to the DataDog API.
   */
  private def pushEventsToDataDog(event: Event, isMetrics: Boolean) {
    if (event != null) {
      val jsonOutString = generateJsonString(event)
      try {
        eventsHttpTransport.send(jsonOutString)
        logger.info("Events Json : " + jsonOutString)

      } catch {
        case ex: org.apache.http.conn.HttpHostConnectException =>
          logger.error("Error in DataDogCollector.pushMetricsToDataDog()" + ex)
          httpTransportConnectionRetry(jsonOutString, eventsHttpTransport, max_re_tyries)
      }
    }
  }

  /**
   * This method is used to re-establish the DataDog-HttpTransport connection if connection
   * exception occurs.
   */
  def httpTransportConnectionRetry(jsonOutString: String, httpTransport: DataDogHttpTransport, retry_count: Int) {

    try {
      while (retry_count != 0) {
        val httpResponseCode = httpTransport.send(jsonOutString)
        logger.info("httpResponseCode :" + httpResponseCode)
        if (httpResponseCode == 202) {
          logger.info("Metrics Json : " + jsonOutString)
          return
        }
      }
    } catch {
      case ex: org.apache.http.conn.HttpHostConnectException =>
        logger.error("Error in DataDogCollector.httpTransportConnectionRetry()" + ex)
        httpTransportConnectionRetry(jsonOutString, httpTransport, retry_count - 1)

    }
  }

  /**
   * This method is used to construct the json response.
   */
  @throws(classOf[Exception])
  private def generateJsonString(classObj: AbstractCaseClass): String = {
    implicit val formats = org.json4s.DefaultFormats
    val returnValue = write(classObj)
    return returnValue
  }

  /**
   * This method is used to get the alert_type for events.
   */
  private def getAlert_type(alert_type: String): String = {
    var alertType = "info";
    if (alert_type == "info" || alert_type == "warning" || alert_type == "success" || alert_type == "error") {
      alertType = alert_type
    }
    alertType
  }

  /**
   * This method is used to get the priority for events.
   */
  private def getPriority(priority_type: String): String = {
    var priority = "normal"
    if (priority_type == "normal" || priority_type == "low") {
      priority = priority_type
    }
    priority
  }
}