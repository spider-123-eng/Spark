package com.spark.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileUtil

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ReadHDFSFolders {
  private val conf = new Configuration()
  val fs = FileSystem.get(conf)
  val uri = conf.get("fs.default.name")

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark-ReadHDFSFolders").setMaster("local[1]"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Hdfs folder path 
    var DATA_PATH = "/user/data/stocks"

    //No of Hdfs folders to read
    val intervalCount = 3
    
    var fileStatus: Array[FileStatus] = fs.listStatus(new Path(uri + DATA_PATH))
    var paths: Array[Path] = FileUtil.stat2Paths(fileStatus)

    var filesWithInterval = getHDFSFoldersBasedOnModtime(intervalCount, fileStatus)

    if (fileStatus != null && filesWithInterval.length >= intervalCount) {
      val dataframeArray = filesWithInterval.map(folder => {
        sqlContext.read.parquet(folder.getPath.toString)
      })

      //Union all the folders and form a single data frame.
      val combinedDataFrame = dataframeArray.reduce((x, y) => x.unionAll(y))

      combinedDataFrame.printSchema()

      println("First Record --> " + combinedDataFrame.first())
    }

  }
  
  //get the folders from HDFS based on the count provided.
  def getHDFSFoldersBasedOnModtime(intervalCount: Int, fileStatus: Array[FileStatus]): Array[FileStatus] = {
    var sortedList: List[FileStatus] = fileStatus.toList.sortWith(_.getModificationTime > _.getModificationTime)
    var returnList: List[FileStatus] = List()
    var itr: Int = 0
    var iterator = sortedList.iterator
    while (iterator.hasNext) {
      var value = iterator.next()
      if (itr < intervalCount) {
        returnList = returnList.::(value)
        itr = itr + 1
      }
    }
    returnList.toArray
  }

}