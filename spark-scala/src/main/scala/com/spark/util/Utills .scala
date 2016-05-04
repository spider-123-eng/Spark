/* Copyright (C) Fujitsu Network Communications, Inc. 2016 - All Rights Reserved
*/

package com.spark.util
import java.text.SimpleDateFormat
import java.net.URI
import java.util.Random
import java.util.Calendar
import java.util.Date
import java.util.TimeZone
import java.time.LocalDate
/**
 * The Object Utills.
 * This Object is used to maintain all the common utility methods .
 * @author RKUMARRE
 */
object Utills extends Serializable {
  
  val tz = TimeZone.getTimeZone("UTC")
  def getTime(): String = {
    val pattern = "dd-MM-yyyy-HH-mm-ss"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    return format.format(new Date())
  }
  def getDay(): String = {
    val pattern = "dd"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    return format.format(new Date())
  }
  def getHour(): String = {
    val pattern = "HH"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    return format.format(new Date())
  }
  def getMinutes(): String = {
    val pattern = "mm"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    return format.format(new Date())
  }
  def getPrevHour(): String = {
    val pattern = "yyyy-MM-dd-HH"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    val date = new Date()
    date.setTime(date.getTime() - 3600 * 1000)
    return format.format(date)
  }
  def getNextHour(): String = {
    val pattern = "HH"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    val date = new Date()
    date.setTime(date.getTime() + 3600 * 1000)
    return format.format(date)
  }
  def getPrevDate(): String = {
    val pattern = "yyyy-MM-dd"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    val date = new Date()
    date.setTime(date.getTime() - 1000 * 60 * 60 * 24)
    return format.format(date)
  }

  def getDateBefore2Days(): String = {
    val pattern = "yyyy-MM-dd-HH"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    val date = new Date();
    val todate = format.format(date)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -2)
    val twodaysBeforDate = cal.getTime()
    return format.format(twodaysBeforDate)
  }
  def getRawDataDateFolder(): String = {
    val pattern = "yyyy-MM-dd-HH-mm"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    return format.format(new Date())
  }
  def getTempHourlyRawDataDateFolder(): String = {
    val pattern = "yyyy-MM-dd-HH"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    val date = new Date()
    date.setTime(date.getTime() + 3600 * 1000)
    return format.format(date)
  }
  def getHourFolder(): String = {
    val pattern = "yyyy-MM-dd-HH"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    return format.format(new Date())
  }
  def getDateFolder(): String = {
    val pattern = "yyyy-MM-dd"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    return format.format(new Date())
  }

  def getTempDayRawDataDateFolder(): String = {
    val pattern = "yyyy-MM-dd"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    return format.format(new Date())
  }
  def getDateBeforeGivenDays(num: Int): String = {
    val pattern = "yyyy-MM-dd"
    val format = new SimpleDateFormat(pattern)
    format.setTimeZone(tz)
    val date = new Date();
    val todate = format.format(date)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -num)
    val twodaysBeforDate = cal.getTime()
    return format.format(twodaysBeforDate)
  }

  def getCurrentDay(): String =
    {
      val date = LocalDate.now()
      val dayname = date.getDayOfWeek().toString()
      return dayname
    }

  def getDurationBreakdown(millis: Long): String = {

    val df = new SimpleDateFormat("HH 'hours', mm 'mins,' ss 'seconds'")
    df.setTimeZone(tz)
    return df.format(new Date(millis))
  }

  def getWeekOfMonth(): Int = {
    return Calendar.getInstance().get(Calendar.WEEK_OF_MONTH)
  }
 
  def getMonthlyAggFolder(): String = {
    val pattern = "MM"
    val format = new SimpleDateFormat(pattern)
    val date = new Date()
    var year = 0
    format.setTimeZone(tz)
    val todate = format.format(date)
    val cal = Calendar.getInstance()
    cal.add(Calendar.MONTH, -1)
    val time = cal.getTime()

    if (format.format(time) == 12) {
      year = cal.get(Calendar.YEAR) - 1
    } else {
      year = cal.get(Calendar.YEAR)
    }

    return year + "-" + format.format(time)
  }

}