package com.datadog.metrics

/**
 * @author revanthreddy
 */
trait MetricsCollector {


   /**
   * This method is used to send metrics to DataDog .
   */
  def sendMetrics(metricName: String, metricValue: Long, tags: collection.mutable.Map[String, Any])

  /**
   * This method is used to send events to DataDog.
   */
  def sendEvents(title: String, text: String, priority: String, alert_type: String, tags: collection.mutable.Map[String, Any])

}