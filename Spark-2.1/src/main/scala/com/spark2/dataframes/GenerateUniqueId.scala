package com.spark2.dataframes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object GenerateUniqueId {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("GenerateUniqueId").master("local[1]").getOrCreate()
    val sc = session.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import session.implicits._
    val df = sc.parallelize(Seq(("Databricks", 20000), ("Spark", 100000), ("Hadoop", 3000))).toDF("word", "count")

    //Option 1 => Using MontotonicallyIncreasingID or ZipWithUniqueId methods
    df.withColumn("uniqueID", monotonicallyIncreasingId).show()

    import org.apache.spark.sql.types.{ StructType, StructField, LongType }
    val df1 = sc.parallelize(Seq(("Databricks", 20000), ("Spark", 100000), ("Hadoop", 3000))).toDF("word", "count")
    val wcschema = df1.schema
    val inputRows = df1.rdd.zipWithUniqueId.map {
      case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)
    }
    val wcID = sqlContext.createDataFrame(inputRows, StructType(StructField("id", LongType, false) +: wcschema.fields))

    wcID.show()

    //Option 2 => Use Row_Number Function

    //With PartitionBy Column:

    val df2 = sc.parallelize(Seq(("Databricks", 20000), ("Spark", 100000), ("Hadoop", 3000))).toDF("word", "count")
    df2.createOrReplaceTempView("wordcount")
    val tmpTable = sqlContext.sql("select row_number() over (partition by word order by count) as rnk,word,count from wordcount")
    tmpTable.show()

    //Without PartitionBy Column:
    val tmpTable1 = sqlContext.sql("select row_number() over (order by count) as rnk,word,count from wordcount")
    tmpTable1.show()
  }

}