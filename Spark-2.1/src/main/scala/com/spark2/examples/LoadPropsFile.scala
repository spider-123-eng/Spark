package com.spark2.dataframes

import org.apache.spark.sql.SparkSession
import java.util.Properties;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkFiles;
import org.apache.hadoop.conf.Configuration

object LoadPropsFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Loading-PropsFile-Spark").master("local[*]")
      .getOrCreate()

    val hdfsConf = new Configuration()
    val fs = FileSystem.get(hdfsConf)

    //file should be in HDFS directory
    val is = fs.open(new Path("/user/centos/input/conf.properties"))
    val prop = new Properties()

    //load properties
    prop.load(is)

    //retrieve properties
    val tidList = prop.getProperty("tidList")
    println("tidList--> " + tidList)

    val topicsList = prop.getProperty("topics")
    println("topicsList--> " + topicsList)
  }
}