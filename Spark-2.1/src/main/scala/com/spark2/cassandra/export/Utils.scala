package com.spark2.cassandra.export

object Utils {

  /**
   * Method to parse the input arguments
   * @param args: Array[String]
   * @return java.util.HashMap[String, String]
   */
  def argsParser(args: Array[String]): java.util.HashMap[String, String] = {
    val result = new java.util.HashMap[String, String]()
    var index = 0
    for (arg <- args) {
      index += 1
      val trimmedArg = arg.trim()
      if (trimmedArg.startsWith("--")) {
        val key = trimmedArg.replaceAll("--", "")
        if (index < args.length) {
          val value = args(index).trim()
          result.put(key, value)
        }
      }
    }
    result
  }

  /**
   * This method is used to parse the timeStamp(2017-09-26 05:00:00.0)
   * @param String: timeStamp
   * @return String: 2017-09
   */
  val setDateMonth: (String) => String = (timeStamp: String) => {
    var date_hour_list = timeStamp.split(" ")
    var date = date_hour_list(0)
    var month = date.split("-")
    month(0) + "-" + month(1)
  }

  /**
   * This method is used to parse the timeStamp(2017-09-26 05:00:00.0)
   * @param String: timeStamp
   * @return String: 2017-09-26-05
   */
  val setDateHour: (String) => String = (timeStamp: String) => {
    var date_hour_list = timeStamp.split(" ")
    var date = date_hour_list(0)
    var month = date.split("-")
    month(0) + "-" + month(1)
    var hour_min_sec = date_hour_list(1).split(":")
    date + "-" + hour_min_sec(0)
  }
}
