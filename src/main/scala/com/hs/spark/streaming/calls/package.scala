package com.hs.spark.streaming

import spray.json.DefaultJsonProtocol

package object calls {

  type kafkaParams = Map[String,Object]

  final case class calls(call_id : Int, call_group_id : Int, call_start_time: String, call_queue_start_time: String, call_queue_end_time: String, call_handling_start_time : String, call_handling_end_time: String, call_end_time: String, department: String, customer_id: String, product: String, call_reason: String)

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val callsFormat = jsonFormat12(calls)
  }

  final case class calls_enriched(call_id : Int, call_end_time: String, call_reason: String, customer_id: String, product: String, department: String, total_call_duration: Long, total_queue_duration: Long, total_handling_duration: Long, call_disconnected: Boolean)

  final case class call_metrics( avg_call_duration: Double, median_call_duration: Double, avg_queue_duration: Double, median_queue_duration: Double, total_disconnected_calls: Int, total_repeat_customers: Int, total_calls: Int)

}
