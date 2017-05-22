package com.spark2.aws

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object Spark_AWS_S3 extends App {
  case class Employee(empid: Int, name: String, dept: String, salary: Double, nop: Int, dttime: String)

  val spark = SparkSession.builder().appName("Spark_AWS_S3").master("local[1]").getOrCreate()
  val sc = spark.sparkContext

  sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "xxxxxxxxxx")
  sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "xxxxxxxxxxxx")

  import spark.implicits._

  val empDF = Seq(
    Employee(123, "revanth", "cloud", 1000, 2, "07-06-2016-06-08-27"),
    Employee(124, "shyam", "finance", 3000, 2, "07-06-2016-06-08-27"),
    Employee(125, "hari", "TAG", 6000, 2, "07-06-2016-06-08-27"),
    Employee(126, "kiran", "cloud", 2000, 2, "08-06-2016-07-08-27"),
    Employee(127, "nandha", "sales", 1000, 2, "08-06-2016-07-08-27"),
    Employee(128, "pawan", "cloud", 1000, 2, "08-06-2016-07-08-27"),
    Employee(129, "kalyan", "conectivity", 1000, 2, "09-06-2016-08-08-27"),
    Employee(121, "satish", "finance", 1000, 2, "09-06-2016-08-08-27"),
    Employee(131, "arun", "cloud", 1000, 2, "09-06-2016-08-08-27"),
    Employee(132, "ram", "cloud", 1000, 2, "10-06-2016-08-08-27"),
    Employee(133, "suda", "conectivity", 1000, 2, "10-06-2016-08-08-27"),
    Employee(134, "sunder", "sales", 1000, 2, "10-06-2016-08-08-27"),
    Employee(135, "charan", "TAG", 1000, 2, "12-06-2016-08-08-27"),
    Employee(136, "ravi", "TAG", 1000, 2, "11-06-2016-08-08-27"),
    Employee(137, "arjun", "cloud", 1000, 2, "11-06-2016-08-08-27")).toDF()

  empDF.coalesce(1).write.format("org.apache.spark.sql.json").mode(SaveMode.Append).save("s3n://snanpsat/emp")

  val empS3DF = spark.read.json("s3n://snanpsat/emp")
  empS3DF.printSchema()

}