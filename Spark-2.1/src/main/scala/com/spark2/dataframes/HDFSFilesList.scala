package com.spark2.dataframes

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object HDFSFilesList {
  private val conf = new Configuration()
  val fs = FileSystem.get(conf)
  val uri = conf.get("fs.default.name")

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Spark-Read-HDFS-Folders").master("local[*]")
      .getOrCreate()

    //Hdfs folder path 
    var DATA_PATH = args(0)

    //No of Hdfs folders to read
    val intervalCount = 1

    var fileStatus: Array[FileStatus] = fs.listStatus(new Path(uri + DATA_PATH))
    var paths: Array[Path] = FileUtil.stat2Paths(fileStatus)

    var filesWithInterval = getHDFSFoldersBasedOnModtime(intervalCount, fileStatus)

    if (fileStatus != null && filesWithInterval.length >= intervalCount) {
      val dataframeArray = filesWithInterval.map(folder => {
        println(folder.getPath.toString)
        val path = folder.getPath.toString
        //spark.read.parquet(folder.getPath.toString)
      })
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