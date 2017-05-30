package com.datadog.metrics

import org.apache.spark.sql.SparkSession

object Spark_Accumulator {
  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local[1]")
      .appName("Spark_Accumulator_Metrics_To_DataDog")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val accum = sc.longAccumulator("total.charecters.counter")
    val words = sc.textFile("input/lines").flatMap(_.split(" "))
    words.foreach(w => accum.add(w.length))

    //setting the metrics tags 
    var tags = collection.mutable.Map[String, Any]()
    tags.put("counter", accum.name.get)
    tags += ("class" -> getClass)
    tags += ("count" -> accum.value, "accum name" -> "total.charecters.counter")
    
    //DataDog API Key needs to be generted by creating account in DataDog 
    val apiKey="947d12f46dead405bf019033434f0xxx"
    //initializing the metrics collector
    val metricsCollector = MetricsCollectorFactory.getDatadogCollector(apiKey, "dev")

    //sending accumulator values as metrics to DataDog
    metricsCollector.sendMetrics(accum.name.get, accum.value, null)

    val badRecords = sc.longAccumulator("bad.records.counter")
    val baddata = sc.textFile("input/badrecords").map(v => v.split(","))
    baddata.foreach(r => { try { r(2).toInt } catch { case e: NumberFormatException => badRecords.add(1) } })

    //sending accumulator values as metrics to DataDog
    metricsCollector.sendMetrics(badRecords.name.get, badRecords.value, tags)

    val acc = sc.longAccumulator("counter.test")
    val baddata1 = sc.textFile("input/badrecords").map(x => acc.add(1))
    baddata1.collect()

    //sending events to DataDog
    metricsCollector.sendEvents("Spark-Events", "Test Run", "normal", "info", tags)

    sc.stop()
  }
}
