package com.spark2.dataframes

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object RecordsCount {
  val conf = new Configuration()
  val fs = FileSystem.get(conf)
  val uri = conf.get("fs.default.name")

  val spark =
    SparkSession.builder()
      .appName("HDFS-Parquet-Records-Count")
      .master("local[*]")
      .getOrCreate()

  //get the folders from HDFS based on the path provided.
  def getHDFSFoldersBasedOnModtime(fileStatus: Array[FileStatus]): Array[FileStatus] = {
    var sortedList: List[FileStatus] = fileStatus.toList.sortWith(_.getModificationTime > _.getModificationTime)
    var returnList: List[FileStatus] = List()
    var itr: Int = 0
    var iterator = sortedList.iterator
    while (iterator.hasNext) {
      var value = iterator.next()
      returnList = returnList.::(value)
      itr = itr + 1
    }
    returnList.toArray
  }

  //get the folders from HDFS based on the path provided.
  def getAllFilePath(filePath: Path, fs: FileSystem): List[String] = {
    var fileList: List[String] = List()
    val fileStatus = fs.listStatus(filePath)
    for (fileStat <- fileStatus) {
      if (fileStat.isDirectory()) {
        fileList = fileList.:::(getAllFilePath(fileStat.getPath(), fs))
        println(fileList.size)
      } else {
        fileList = fileList.+:(fileStat.getPath().toString())
      }
    }

    return fileList
  }

  def main(args: Array[String]) {

    //Hdfs folder path 
    var DATA_PATH = args(0)

    //one way of doing  
    var fileStatus: Array[FileStatus] = fs.listStatus(new Path(uri + DATA_PATH))
    var paths: Array[Path] = FileUtil.stat2Paths(fileStatus)
    var recordCount = 0l
    var filesWithInterval = getHDFSFoldersBasedOnModtime(fileStatus)

    if (fileStatus != null) {
      val dataframeArray = filesWithInterval.map(folder => {
        val path = folder.getPath.toString
        println("path : " + path)
        recordCount = recordCount + spark.read.parquet(path).count()
      })
    }
    println("Total-Records-Count : " + recordCount)

    //or alternate way
    var recordsCount = 0l
    val p = new Path(DATA_PATH);
    val pathList = getAllFilePath(p, fs)
    for (path <- pathList) {
      println("path : " + path)
      recordsCount = recordsCount + spark.read.parquet(path).count()
    }
    println("Total-Records-Count : " + recordsCount)

  }
}