package com.hs.spark.streaming

import spray.json.DefaultJsonProtocol

package object calls {

  type kafkaParams = Map[String,Object]

  final case class calls(call_id : Int, call_group_id : Int, call_start_time: String, call_queue_start_time: String, call_queue_end_time: String, call_handling_start_time : String, call_handling_end_time: String, call_end_time: String, department: String, customer_id: String, product: String, call_reason: String)

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val callsFormat = jsonFormat12(calls)
    implicit val callsEnrichedFormat = jsonFormat12(calls_enriched)
    implicit val metricsFormat = jsonFormat7(metrics)
    implicit val callsMetricsFormat = jsonFormat3(call_metrics)
    implicit val top5CallReasonFormat = jsonFormat5(top_5_call_reason)
  }

  case class metrics(total_call_duration: Long, total_queue_duration : Long, total_handling_duration: Long, call_disconnected: Int, total_calls: Int, indRepeatCustomers: Int, total_anamolus_calls: Int )

  final case class calls_enriched(call_id : Int, call_end_time: String, call_reason: String, customer_id: String, product: String, department: String, total_call_duration: Long, total_queue_duration: Long, total_handling_duration: Long, call_disconnected: Int, repeat_call_counter: Int, Anomalous_call: Int)

  final case class call_metrics(period_start_time: Long, period_end_time: Long, metrics: metrics)

  final case class top_5_call_reason(period_start_time: Long, period_end_time: Long,call_reason: String, total_calls: Int, rank: Int)

}
