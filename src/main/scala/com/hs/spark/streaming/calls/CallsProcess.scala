package com.hs.spark.streaming.calls

object CallsProcess {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

  def getTimeDiff(dateStr1: String, dateStr2: String): Long= {
    val date1 = format.parse(dateStr1)
    val date2 = format.parse(dateStr2)
    date2.getTime - date1.getTime
  }

  def calculateTimeUnits(calls: calls) = {
    (calls.call_id,getTimeDiff(calls.call_start_time, calls.call_end_time),getTimeDiff(calls.call_queue_start_time, calls.call_queue_end_time), getTimeDiff(calls.call_handling_start_time, calls.call_handling_end_time))
  }





}
