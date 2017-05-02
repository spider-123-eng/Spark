package com.datadog.metrics

object CaseClasses {

  //case classes for events and metrics construction
  case class SeriesList(series: List[Series]) extends AbstractCaseClass
  case class Series(metric: String, `type`: String, points: List[List[Long]], tags: List[String]) extends AbstractCaseClass
  case class Event(title: String, text: String, priority: String, alert_type: String, date_happened: Long, tags: List[String]) extends AbstractCaseClass

}