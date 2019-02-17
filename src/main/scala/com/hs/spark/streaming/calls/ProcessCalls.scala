package com.hs.spark.streaming.calls

object ProcessCalls {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

  def getTimeDiff(dateStr1: String, dateStr2: String): Long= {
    val date1 = format.parse(dateStr1)
    val date2 = format.parse(dateStr2)
    date2.getTime - date1.getTime
  }

  def enrichCalls(calls: calls): calls_enriched = {
    val callsTimeDiff = (getTimeDiff(calls.call_start_time, calls.call_end_time),getTimeDiff(calls.call_queue_start_time, calls.call_queue_end_time), getTimeDiff(calls.call_handling_start_time, calls.call_handling_end_time))
    val indDisconnected : Boolean = callsTimeDiff._3 match {
      case x if x > 0 => false
      case x if x <=0 => true
    }
    calls_enriched(calls.call_id,calls.call_end_time,calls.call_reason,calls.customer_id,calls.product,calls.department, callsTimeDiff._1,callsTimeDiff._2,callsTimeDiff._3,indDisconnected)
  }

  def calculatemetrics = {
      }







}
