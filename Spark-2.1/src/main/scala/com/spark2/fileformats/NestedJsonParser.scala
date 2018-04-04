package com.spark2.fileformats
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
object NestedJsonParser extends App {

  val session = SparkSession.builder().appName("Spark-JsonParser")
    .master("local[1]").getOrCreate()

  val schema = StructType(List(
    StructField("queryResults", StructType(
      List(StructField("searchResponse", StructType(
        List(StructField("response", StructType(
          List(StructField("docs", ArrayType(StructType(
            List(
              StructField("appCustNumber", StringType, nullable = true),
              StructField("transactions", ArrayType(StructType(
                List(
                  StructField("code", StringType, nullable = true),
                  StructField("description", StringType, nullable = true),
                  StructField("recordDate", StringType, nullable = true))))))))))))))))))))

  val dff = session.read.schema(schema).json("input/nested.json")
  println(dff.printSchema())

  val dfContent = dff.select(explode(dff("queryResults.searchResponse.response.docs.transactions"))).toDF("transaction")
  val code = dfContent.select("transaction.code")
  code.show(false)

  val dfFinal = dfContent.select("transaction.code", "transaction.description", "transaction.recordDate")
  dfFinal.show(false)

}