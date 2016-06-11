package com.spark.util

import org.apache.log4j.Logger
import org.apache.log4j.xml.DOMConfigurator

trait LogHelper {
  DOMConfigurator.configure(Utills.DATA_PATH +"log4j_conf.xml")
  val loggerName = this.getClass.getName
  final val logger = Logger.getLogger(loggerName)
}