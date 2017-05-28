package com.spark2.examples
import org.apache.spark.sql.SparkSession

object FilterEmpty extends App {

  private def checkNullForFloat(value: String): Float = {
    if (!"".equals(value)) {
      return value.toFloat;
    }
    return 0;
  }
  case class Product(product_id: Int, product_category_id: Int, product_name: String, product_description: String, product_price: Float, product_image: String)
  val session = SparkSession.builder().appName("Spark-FilterEmpty")
    .master("local[1]").getOrCreate()

  import session.implicits._
  val rawRDD = session.sparkContext.textFile("input/product")

  val dummyRDD = rawRDD.map(_.split("\\,")).map(p => (p(0).toInt, p(1)toInt, p(2), p(3), p(4), p(5)))
  dummyRDD.filter(x => (x._5 != null) && (x._5.length > 0)).toDF().show()

  //OR
  val prodRDD = rawRDD.map(_.split("\\,")).map(p => Product(p(0).toInt, p(1)toInt, p(2), p(3), checkNullForFloat(p(4)), p(5)))

  //removing the products that have product_price = 0.0
  val resDF = prodRDD.filter(x => x.product_price != 0.0).toDF()

  resDF.sort($"product_price".desc).show()

}
