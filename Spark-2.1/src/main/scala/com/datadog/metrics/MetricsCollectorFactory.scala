package com.datadog.metrics

/**
 * @author revanthreddy
 */
object MetricsCollectorFactory {

  def getDatadogCollector(apikey: String, env: String): MetricsCollector = new DataDogCollector(apikey, env)

}