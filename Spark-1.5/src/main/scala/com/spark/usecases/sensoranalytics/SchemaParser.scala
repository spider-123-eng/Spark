package com.spark.usecases.sensoranalytics

import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

import scala.util.Try

object SchemaParser {

  def parse(eachRow: String): Option[SensorRecord] = {
    val columns = eachRow.split(",")
    //println("columns --->" + columns(0) +","+ columns(1) +","+ columns(2) +","+ columns(3))
    Try {
      if (columns.length == 5) {
        Option(SensorRecord(createDate(columns(0)), columns(1), columns(2), columns(3), columns(4)))
      } else {
        None
      }
    }.getOrElse(None)
  }

  def createDate(input: String) = {
    val columns = input.split(" ")
    val pattern = "YYYY/MM/dd HH"
    DateTimeFormat.forPattern(pattern)
      .parseDateTime(columns(0) + " " + columns(1).split(":")(0))
      .withZone(DateTimeZone.getDefault())
  }

  def createDelay(input: String): Double = {
    val delay_regex = """[^\d|.]*([0-9\\.]+)\s*(ms|.*)""".r

    input match {
      case delay_regex(value, unit) => {
        if (unit.equalsIgnoreCase("ms")) {
          value.toDouble
        } else {
          0
        }
      }
    }
  }
}