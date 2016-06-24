package com.spark.examples
import com.spark.util._
import scala.reflect.runtime.universe
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import au.com.bytecode.opencsv.CSVParser;
import org.apache.spark.rdd.RDD

object DataFramesRollup {
  case class dataBean(week: String, campaignType: String, campaign: String, account: String, brandUnBrand: String, category: String, impressions: Int, clicks: Int, cost: Double, engagements: String, patientJourney: String, device: String, indication: String, country: String, region: String, metroArea: String)
  case class FinalResultRollup(week: String, campaignType: String, campaign: String, account: String, brandUnBrand: String, category: String, impressions: Long, clicks: Long, cost: Double, engagements: String, patientJourney: String, device: String, indication: String, country: String, region: String, metroArea: String)
  var parser = new CSVParser(',');

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-DataFrameRollup").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //populating dataBean object

    val objDataBean = sc.textFile("file:/E:/Software/Spark/data/campaign.csv")
    val first = objDataBean.first()
    val header: RDD[String] = sc.parallelize(Array(first))

    val dropHeaderRDD = objDataBean.mapPartitions(_.drop(1))
    val filterEmptyRowsRDD = dropHeaderRDD.filter(line => isNonEmptyLine(line))

    //converting dataBean object to Data Frame.
    val objDataBeanDF = dropHeaderRDD.map(getTokens(_))
      .map(x => dataBean(x(0), x(1), x(2), x(3), x(4), x(5), checkNullForInt(x(6)), checkNullForInt(x(7)), checkNullForDouble(x(8)), x(9), x(10), x(11), x(12), x(13), x(14), x(15))).toDF()

    val aggDF = objDataBeanDF.groupBy("week", "campaignType", "campaign", "account", "brandUnBrand", "category", "impressions", "clicks", "cost", "engagements", "patientJourney", "device", "indication", "country", "region", "metroArea").
      agg(sum(objDataBeanDF.col("impressions")), sum(objDataBeanDF.col("clicks")), sum(objDataBeanDF.col("cost")))

    val finalDF = aggDF.map(x => FinalResultRollup(x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4), x.getString(5), x.getLong(16), x.getLong(17), x.getDouble(18), x.getString(9), x.getString(10), x.getString(11), x.getString(12), x.getString(13), x.getString(14), x.getString(15))).toDF()

    val finalRDD = finalDF.rdd.map(row => checkForComma(row(0)) + "," + checkForComma(row(1)) + "," + checkForComma(row(2)) + "," + checkForComma(row(3)) + "," + checkForComma(row(4)) + "," + checkForComma(row(5)) + "," + row(6) + "," + row(7) + "," + row(8) + "," + checkForComma(row(9)) + "," + checkForComma(row(10)) + "," + checkForComma(row(11)) + "," + checkForComma(row(12)) + "," + checkForComma(row(13)) + "," + checkForComma(row(14)) + "," + checkForComma(row(15)))

    header.union(finalRDD).coalesce(1, true).saveAsTextFile("E:/Software/Spark/data/" + Utills.getTime())
  }

  private def checkNullForInt(value: String): Integer = {
    if (!"".equals(value)) {
      return value.toInt;
    }
    return 0;
  }

  private def checkNullForDouble(value: String): Double = {
    if (!"".equals(value)) {
      return value.toDouble;
    }
    return 0.0;
  }

  private def getTokens(value: String): Array[String] = {
    if (!"".equals(value)) {
      var tokens: Array[String] = parser.parseLine(value);
      if (tokens.length != 16) {
        println("Line = " + value + ",Token Length= " + tokens.length)
      }
      return tokens;
    }
    return null;
  }

  private def checkForComma(value: Any): String = {
    if (!"".equals(value) && value.toString().indexOf(',') > -1) {
      val newvalue = "\"" + value + "\"";
      return newvalue;
    }
    return value.toString();
  }
  private def isNonEmptyLine(value: String): Boolean = {
    if ("".equals(value)) {
      return false;
    } else {
      if (value.length() > 20) {
        return true;
      } else return false;
    }
    return true;
  }

}