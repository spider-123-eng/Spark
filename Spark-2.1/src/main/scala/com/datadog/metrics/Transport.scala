package com.datadog.metrics
import java.io.Closeable


/**
 * The transport layer for pushing metrics to datadog
 */
trait Transport extends Closeable {

  /**
   * Build a request context.
   */
  def send(jsonData: String): Int

  /**
   * Send the request to datadog
   */
  def sendToDataDog(transport: DataDogHttpTransport,jsonData: String): Int

}