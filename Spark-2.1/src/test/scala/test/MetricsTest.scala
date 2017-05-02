package test

import org.apache.spark.sql.SparkSession
import com.rasa.metrics.MetricsCollectorFactory

object MetricsTest {
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
    var metricsTags = collection.mutable.Map[String, Any]()
    metricsTags.put("accum name", accum.name.get)
    metricsTags.put("accum value", accum.value)

    //initializing the metrics collector
    val metricsCollector = MetricsCollectorFactory.getDatadogCollector("947d12f46dead405bf019033434f0cba", "dev")

    //sending accumulator values as metrics to DataDog
    metricsCollector.sendMetrics(accum.name.get, accum.value, metricsTags)

    val badRecords = sc.longAccumulator("bad.records.counter")
    val baddata = sc.textFile("input/badrecords").map(v => v.split(","))
    baddata.foreach(r => { try { r(2).toInt } catch { case e: NumberFormatException => badRecords.add(1) } })

    //sending accumulator values as metrics to DataDog
    metricsCollector.sendMetrics(badRecords.name.get, badRecords.value, null)

    val acc = sc.longAccumulator("counter.test")
    val baddata1 = sc.textFile("input/badrecords").map(x => acc.add(1))
    baddata1.collect()

    //setting the event tags 
    var eventTags = collection.mutable.Map[String, Any]()
    eventTags.put("accum name", acc.name.get)
    eventTags.put("accum value", acc.value)

    //sending events to DataDog
    metricsCollector.sendEvents("DataDog Event Test", "Sending events", "normal", "info", eventTags)

    sc.stop()
  }
}