package com.spark.usecases
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import com.spark.util._
import org.apache.spark.sql.functions.udf
object FlightDataAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FlightDataAnalysis").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // Create a DataFrame from datasets
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(Utills.DATA_PATH + "flights.csv") // Read all flights

    // Print the schema in a tree format
    df.printSchema

    // Show a subset of columns with "select"
    df.select("UniqueCarrier", "FlightNum", "DepDelay", "ArrDelay", "Distance").show

    // Create a DataFrame containing Flights with delayed Departure by more than 15 min using "filter"
    val delayedDF = df.select("UniqueCarrier", "DepDelay").filter($"DepDelay" > 15).cache
    delayedDF.show

    // Print total number of delayed flights
    println("Total Number of Delayed Flights: " + delayedDF.count)

    // Define a UDF to find delayed flights

    // Assume:
    //  if ArrDelay is not available then Delayed = False
    //  if ArrDelay > 15 min then Delayed = True else False

    val isDelayedUDF = udf((time: String) => if (time == "NA") 0 else if (time.toInt > 15) 1 else 0)

    // Define a new DataFrame that contains a subset of the original columns and a new column "IsDelayed" by applying a UDF
    // isDelayed()  on "DepDelay" column

    val updatedDF = df.select($"Year", $"Month", $"DayofMonth", $"DayOfWeek", $"CRSDepTime", $"UniqueCarrier", $"FlightNum",
      $"DepDelay", $"Origin", $"Dest", $"TaxiIn", $"TaxiOut", $"Distance",
      isDelayedUDF($"DepDelay").alias("IsDelayed")).cache

    updatedDF.show // Notice new column "IsDelayed"

    //Calculate Percentage of Delayed Flights
    updatedDF.agg((sum("IsDelayed") * 100 / count("DepDelay")).alias("Percentage of Delayed Flights")).show

    //Find Avg Taxi-out
    // Show only Origin, Dest, and TaxiOut columns
    updatedDF.select("Origin", "Dest", "TaxiOut").groupBy("Origin", "Dest").agg(avg("TaxiOut").alias("AvgTaxiOut")).orderBy(desc("AvgTaxiOut")).show(10)

  }
  //Dataset Description

  /*     Name	Description
      1	 Year	1987-2008
      2	 Month	1-12
      3	 DayofMonth	1-31
      4	 DayOfWeek	1 (Monday) - 7 (Sunday)
      5	 DepTime	actual departure time (local, hhmm)
      6	 CRSDepTime	scheduled departure time (local, hhmm)
      7	 ArrTime	actual arrival time (local, hhmm)
      8	 CRSArrTime	scheduled arrival time (local, hhmm)
      9	 UniqueCarrier	unique carrier code
      10	FlightNum	flight number
      11	TailNum	plane tail number
      12	ActualElapsedTime	in minutes
      13	CRSElapsedTime	in minutes
      14	AirTime	in minutes
      15	ArrDelay	arrival delay, in minutes
      16	DepDelay	departure delay, in minutes
      17	Origin	origin IATA airport code
      18	Dest	destination IATA airport code
      19	Distance	in miles
      20	TaxiIn	taxi in time, in minutes
      21	TaxiOut	taxi out time in minutes
      22	Cancelled	was the flight cancelled?
      23	CancellationCode	reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
      24	Diverted	1 = yes, 0 = no
      25	CarrierDelay	in minutes
      26	WeatherDelay	in minutes
      27	NASDelay	in minutes
      28	SecurityDelay	in minutes
      29	LateAircraftDelay	in minutes
      */
}