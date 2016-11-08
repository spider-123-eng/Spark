package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partitioner

object CustomPartitioner {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Spark-Custom-Partitioner").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    val inputFile = sc.textFile("E:/Software/Spark/data/partitioner.txt")

    //create paired RDD 
    val pairedData = inputFile.flatMap(x => x.split(" ")).map(x => (x, 1))

    //Define custom pertitioner for paired RDD
    val partitionedData = pairedData.partitionBy(new MyCustomerPartitioner(2)).map(f => f._1)

    //verify result using mapPartitionWithIndex
    val finalOut = partitionedData.mapPartitionsWithIndex {
      (partitionIndex, dataIterator) => dataIterator.map(dataInfo => (dataInfo + " is located in  " + partitionIndex + " partition."))
    }
    //Save Output in HDFS
    finalOut.saveAsTextFile("E:/Software/Spark/data/partitionOutput")

  }
}
class MyCustomerPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int =
    {
      val out = toInt(key.toString)
      out
    }

  override def equals(other: Any): Boolean = other match {
    case dnp: MyCustomerPartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }

  def toInt(s: String): Int =
    {
      try {
        s.toInt
        0
      } catch {
        case e: Exception => 1

      }
    }
}