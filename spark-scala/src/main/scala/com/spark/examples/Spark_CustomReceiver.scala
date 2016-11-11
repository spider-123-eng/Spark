package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import java.io.BufferedReader
import java.net.Socket
import java.io.InputStreamReader
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel

case class Order(id: Int, total: Int, items: List[Item] = null)
case class Item(id: Int, cost: Int)
object Spark_CustomReceiver {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Spark-CustomReceiver").setMaster("local[1]") //.set("spark.sql.warehouse.dir", "file:///D:/Spark-WorkSpace/Spark-Windows/spark-warehouse")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    // Create an input stream with the custom receiver on target ip:port
    val stream: DStream[Order] = ssc.receiverStream(new OrderReceiver(args(0), args(1).toInt))

    stream.foreachRDD { rdd =>
      rdd.foreach(order => {
        println("Order ID : " + order.id)
        println("************* Item for each order ************* ")
        order.items.foreach(println)
      })
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

class OrderReceiver(host: String, port: Int) extends Receiver[Order](StorageLevel.MEMORY_ONLY) {

  override def onStart(): Unit = {

    println("starting...")

    val thread = new Thread("Receiver") {
      override def run() { receive() }
    }

    thread.start()
  }

  override def onStop(): Unit = stop("I am done")

  def receive() = {
    val socket = new Socket(host, port)
    var currentOrder: Order = null
    var currentItems: List[Item] = null

    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))

    while (!isStopped()) {
      var userInput = reader.readLine()

      if (userInput == null) stop("Stream has ended")
      else {
        val parts = userInput.split(" ")

        if (parts.length == 2) {
          if (currentOrder != null) {
            store(Order(currentOrder.id, currentOrder.total, currentItems))
          }

          currentOrder = Order(parts(0).toInt, parts(1).toInt)
          currentItems = List[Item]()
        } else {
          currentItems = Item(parts(0).toInt, parts(1).toInt) :: currentItems
        }
      }
    }
  }
}