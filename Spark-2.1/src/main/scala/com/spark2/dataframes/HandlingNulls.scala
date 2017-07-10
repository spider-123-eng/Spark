package com.spark2.dataframes

import org.apache.spark.sql.SparkSession

object HandlingNulls {

  val spark = SparkSession.builder().appName("Handling-Nulls").master("local[*]")
    .getOrCreate()

  case class employee(
    employee_id: Int, first_name: String, last_name: String,
    email: String, phone_number: String, hire_date: String,
    job_id: String, salary: Float, commission_pct: Float,
    manager_id: Int, department_id: Int)

  private def checkNullForFloat(value: String): Float = {
    if (!"null".equals(value)) {
      return value.toFloat;
    } else if (!"".equals(value)) {
      return value.toFloat;
    }
    return 0f;
  }
  private def checkNullForInt(value: String): Int = {
    if (!"null".equals(value)) {
      return value.toInt;
    } else if (!"".equals(value)) {
      return value.toInt;
    }
    return 0;
  }
  def main(args: Array[String]): Unit = {

    val employeeData = spark.sparkContext.textFile("input/emp.txt")
    import spark.implicits._

    val employeeDF = employeeData.map(
      rec => {
        var d = rec.split(",")
        employee(d(0).toInt,
          d(1), d(2), d(3), d(4), d(5), d(6), d(7).toFloat,
          checkNullForFloat(d(8)),
          checkNullForInt(d(9)),
          checkNullForInt(d(10)))

      }).toDF()

      
      //or another way of filtering null columns 
    val employeeDF1 = employeeData.map(
      rec => {
        var d = rec.split(",")
        employee(d(0).toInt,
          d(1), d(2), d(3), d(4), d(5), d(6), d(7).toFloat,
          if (d(8).asInstanceOf[Any] != "null") d(8).toFloat else 0F,
          if (d(9).asInstanceOf[Any] != "null") d(9).toInt else 0,
          if (d(10).asInstanceOf[Any] != "null") d(10).toInt else 0)
      }).toDF()

    employeeDF.show()
  }
}