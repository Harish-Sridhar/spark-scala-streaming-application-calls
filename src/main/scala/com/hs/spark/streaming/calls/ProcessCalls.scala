package com.hs.spark.streaming.calls

import org.apache.spark.rdd.{RDD, RDDBarrier}
import org.apache.spark.streaming.{Seconds, Time}

object ProcessCalls {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

  def getTimeDiff(dateStr1: String, dateStr2: String): Long= {
    val date1 = format.parse(dateStr1)
    val date2 = format.parse(dateStr2)
    date2.getTime - date1.getTime
  }

  def enrichCalls(calls: calls): calls_enriched = {
    val callsTimeDiff = (getTimeDiff(calls.call_start_time, calls.call_end_time),getTimeDiff(calls.call_queue_start_time, calls.call_queue_end_time), getTimeDiff(calls.call_handling_start_time, calls.call_handling_end_time))
    val indDisconnected : Int = callsTimeDiff._3 match {
      case x if x > 0 => 0
      case x if x <=0 => 1
    }
    calls_enriched(calls.call_id,calls.call_end_time,calls.call_reason,calls.customer_id,calls.product,calls.department, callsTimeDiff._1,callsTimeDiff._2,callsTimeDiff._3,indDisconnected, 0, 0)
  }

  def calculateRepeatCustomer(rdd : RDD[calls_enriched]) : RDD[(String, metrics)] ={
    rdd.map(calls_enriched => (calls_enriched.customer_id, metrics(calls_enriched.total_call_duration, calls_enriched.total_queue_duration,calls_enriched.total_handling_duration,calls_enriched.call_disconnected, 1, 0, 0)))
      .reduceByKey({
        case (metrics1: metrics, metrics2: metrics) =>
          metrics(metrics1.total_call_duration+metrics2.total_call_duration, metrics1.total_queue_duration+metrics2.total_queue_duration, metrics1.total_handling_duration+metrics2.total_handling_duration, metrics1.call_disconnected+metrics2.call_disconnected, metrics1.total_calls+metrics2.total_calls, metrics1.indRepeatCustomers+metrics2.indRepeatCustomers, metrics1.total_anamolus_calls+metrics2.total_anamolus_calls)
      })
      .map(x => {
        val metrics1 :metrics = x._2.total_calls match {
          case total if (total > 4 ) => x._2.copy(indRepeatCustomers=1)
          case _ => x._2.copy()
        }
        (x._1, metrics1)
      })
  }

  def calculateMetrics(rdd : RDD[metrics], time :Time):RDD[call_metrics] = {
    val aggCallMetrics : RDD[call_metrics] =rdd.map(x=> (1,x))
      .reduceByKey({
      case (metrics1: metrics, metrics2: metrics) =>
        metrics(metrics1.total_call_duration+metrics2.total_call_duration, metrics1.total_queue_duration+metrics2.total_queue_duration, metrics1.total_handling_duration+metrics2.total_handling_duration, metrics1.call_disconnected+metrics2.call_disconnected, metrics1.total_calls+metrics2.total_calls, metrics1.indRepeatCustomers+metrics2.indRepeatCustomers, metrics1.total_anamolus_calls+metrics2.total_anamolus_calls)
    })
      .map(x=> call_metrics(time.-(Seconds(60)).milliseconds, time.milliseconds, x._2))
    aggCallMetrics
  }

  def calculateTop5CallReasons(rdd : RDD[calls_enriched], time :Time):RDD[top_5_call_reason] = {
    rdd.map(calls_enriched => (calls_enriched.call_reason, 1))
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .zipWithIndex()
      .filter(x => x._2<5)
      .map(x => top_5_call_reason(time.-(Seconds(60)).milliseconds, time.milliseconds, x._1._1, x._1._2, x._2.toInt+1))
  }







}
