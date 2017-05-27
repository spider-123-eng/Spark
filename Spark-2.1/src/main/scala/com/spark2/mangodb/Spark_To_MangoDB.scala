package com.spark2.mangodb

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import com.mongodb.casbah.{ WriteConcern => MongodbWriteConcern }
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._

object Spark_To_MangoDB {

  case class Student(name: String, age: Int, gender: String)

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Spark_To_MangoDB").master("local[1]").getOrCreate()

    //To save a DataFrame in MongoDB
    val saveConfig = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "dev", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))

    val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(
      List(Student("ravali", 27, "female"), Student("abc", 34, "male"))))

    df.saveToMongodb(saveConfig.build)

    //fromMongoDB() function to read from MongoDB and transform it to a DataFrame
    val builder = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "dev", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> "normal"))
    val readConfig = builder.build
    val mongoRDD = spark.sqlContext.fromMongoDB(readConfig)
    mongoRDD.createTempView("students")

    val dataFrame = spark.sql("SELECT name, age,gender FROM students")
    dataFrame.show

    //Using DataFrameWriter
    import org.apache.spark.sql._
    val options = Map("host" -> "localhost:27017", "database" -> "dev", "collection" -> "students")
    val dfw: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(List(Student("ravi", 30, "female"))))
    dfw.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(options).save()
    val resDF = spark.read.format("com.stratio.datasource.mongodb").options(options).load
    resDF.show

  }
}