package com.hs.spark.streaming

import spray.json.DefaultJsonProtocol

package object calls {

  type kafkaParams = Map[String,Object]

  final case class calls(call_id : Int, call_group_id : Int, call_start_time: String, call_queue_start_time: String, call_queue_end_time: String, call_handling_start_time : String, call_handling_end_time: String, call_end_time: String, department: String, customer_id: List[String], product: String, call_reason: String)

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val callsFormat = jsonFormat12(calls)
  }

}
