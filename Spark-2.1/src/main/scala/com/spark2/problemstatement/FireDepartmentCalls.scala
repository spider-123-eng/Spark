package com.spark2.problemstatements

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, BooleanType }
object FireDepartmentCalls {

  val spark = SparkSession.builder.appName("Fire-Department-Calls").master("local[*]")
    .getOrCreate

  def main(args: Array[String]): Unit = {

    val fireServiceCallsDF1 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED").option("inferSchema", "true")
      .csv("input/FireDepartmentCalls.csv")

    fireServiceCallsDF1.printSchema()

    fireServiceCallsDF1.show(true)

    //Inferring the schema works for ad hoc analysis against smaller datasets. But when working on multi-TB+ data, 
    //it"s better to provide an explicit pre-defined schema manually, so there"s no inferring cost

    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("ReceivedDtTm", StringType, true),
      StructField("EntryDtTm", StringType, true),
      StructField("DispatchDtTm", StringType, true),
      StructField("ResponseDtTm", StringType, true),
      StructField("OnSceneDtTm", StringType, true),
      StructField("TransportDtTm", StringType, true),
      StructField("HospitalDtTm", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("ZipcodeofIncident", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumberofAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("Unitsequenceincalldispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("NeighborhoodDistrict", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true)))

    val fireServiceCallsDF = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .schema(fireSchema)
      .csv("input/Fire_Department_Calls.csv")

    import spark.implicits._
    //Print just the column names in the DataFrame:
    //   fireServiceCallsDF.columns.foreach { x => println(x) }

    //Q-1) How many different types of calls were made to the Fire Department?
    fireServiceCallsDF.select("CallType").show(5)

    // Add the .distinct() transformation to keep only distinct rows
    // The False below expands the ASCII column width to fit the full text in the output
    fireServiceCallsDF.select("CallType").distinct().show(5, false)

    //Q-2) How many incidents of each call type were there?   
    fireServiceCallsDF.select("CallType").groupBy("CallType").count().orderBy($"count".desc).show

    //Q-3) How many years of Fire Service Calls is in the data file?

    //Notice that the date or time columns are currently being interpreted as strings, rather than date or time objects:
    val from_pattern1 = "MM/dd/yyyy"
    val to_pattern1 = "yyyy-MM-dd"

    val from_pattern2 = "MM/dd/yyyy hh:mm:ss aa"
    val to_pattern2 = "MM/dd/yyyy hh:mm:ss aa"

    import org.apache.spark.sql.functions._

    //Let's use the unix_timestamp() function to convert the string into a timestamp:
    val fireServiceCallsTsDF = fireServiceCallsDF
      .withColumn("CallDateTS", unix_timestamp(fireServiceCallsDF("CallDate"), from_pattern1).cast("timestamp")).drop("CallDate")
      .withColumn("WatchDateTS", unix_timestamp(fireServiceCallsDF("WatchDate"), from_pattern1).cast("timestamp")).drop("WatchDate")
      .withColumn("ReceivedDtTmTS", unix_timestamp(fireServiceCallsDF("ReceivedDtTm"), from_pattern2).cast("timestamp")).drop("ReceivedDtTm")
      .withColumn("EntryDtTmTS", unix_timestamp(fireServiceCallsDF("EntryDtTm"), from_pattern2).cast("timestamp")).drop("EntryDtTm")
      .withColumn("DispatchDtTmTS", unix_timestamp(fireServiceCallsDF("DispatchDtTm"), from_pattern2).cast("timestamp")).drop("DispatchDtTm")
      .withColumn("ResponseDtTmTS", unix_timestamp(fireServiceCallsDF("ResponseDtTm"), from_pattern2).cast("timestamp")).drop("ResponseDtTm")
      .withColumn("OnSceneDtTmTS", unix_timestamp(fireServiceCallsDF("OnSceneDtTm"), from_pattern2).cast("timestamp")).drop("OnSceneDtTm")
      .withColumn("TransportDtTmTS", unix_timestamp(fireServiceCallsDF("TransportDtTm"), from_pattern2).cast("timestamp")).drop("TransportDtTm")
      .withColumn("HospitalDtTmTS", unix_timestamp(fireServiceCallsDF("HospitalDtTm"), from_pattern2).cast("timestamp")).drop("HospitalDtTm")
      .withColumn("AvailableDtTmTS", unix_timestamp(fireServiceCallsDF("AvailableDtTm"), from_pattern2).cast("timestamp")).drop("AvailableDtTm")

    //fireServiceCallsTsDF.printSchema()
    //fireServiceCallsTsDF.show(5,false)

    //calculate how many distinct years of data is in the CSV file ?
    fireServiceCallsTsDF.select(year(fireServiceCallsTsDF("CallDateTS"))).distinct().orderBy($"year(CallDateTS)".desc).show()

    //Q-4) How many service calls were logged in the past 7 days?

    fireServiceCallsTsDF.select((dayofyear(fireServiceCallsTsDF("CallDateTS"))), (year(fireServiceCallsTsDF("CallDateTS")))).show

    fireServiceCallsTsDF.filter(year(fireServiceCallsTsDF("CallDateTS")) === "2017").
      filter(dayofyear(fireServiceCallsTsDF("CallDateTS")) >= 150).select(dayofyear(fireServiceCallsTsDF("CallDateTS")))
      .distinct().orderBy($"dayofyear(CallDateTS)".desc).show()

    fireServiceCallsTsDF.filter(year(fireServiceCallsTsDF("CallDateTS")) === "2017").
      filter(dayofyear(fireServiceCallsTsDF("CallDateTS")) >= 100).groupBy(dayofyear(fireServiceCallsTsDF("CallDateTS")))
      .count().orderBy($"dayofyear(CallDateTS)".desc).show()

    //println(fireServiceCallsTsDF.rdd.getNumPartitions)

    fireServiceCallsTsDF.repartition(6).createOrReplaceTempView("fireServiceVIEW")
    spark.catalog.cacheTable("fireServiceVIEW")
    //println(spark.table("fireServiceVIEW").count())
    //println(spark.catalog.isCached("fireServiceVIEW"))
    import spark.sql

    //sql("SELECT * FROM fireServiceVIEW").show()

    //Q-5) Which neighborhood in SF generated the most calls last year?
    sql("SELECT NeighborhoodDistrict, count(NeighborhoodDistrict) AS Neighborhood_Count FROM fireServiceVIEW WHERE year(CallDateTS) == '2017' GROUP BY NeighborhoodDistrict ORDER BY Neighborhood_Count DESC LIMIT 5").show()

    //Q-6) What was the primary non-medical reason most people called the fire department from the Tenderloin last year?

    val incidentsDF = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED").option("inferSchema", "true")
      .csv("input/Fire_Incidents.csv")
      .withColumnRenamed("Incident Number", "IncidentNumber").cache()

    // println(incidentsDF.count())

    val fireServiceDF = spark.table("fireServiceVIEW")
    val joinedDF = fireServiceDF.join(incidentsDF, fireServiceDF.col("IncidentNumber") === incidentsDF.col("IncidentNumber"))

    joinedDF.filter(year(fireServiceDF("CallDateTS")) === "2000")
      .filter(fireServiceDF.col("NeighborhoodDistrict") === "Tenderloin")
      .groupBy("Primary Situation").count().orderBy(desc("count")).show(10, false)
  }
}