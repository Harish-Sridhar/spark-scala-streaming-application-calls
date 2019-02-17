package com.hs.spark.streaming.calls

import com.typesafe.config.{Config, ConfigFactory}
import MyJsonProtocol._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.joda.time.DateTime
import spray.json.JsonParser

object Application {

  def main(args: Array[String]): Unit = {


    // Loading configurations needed.
    val config: Config = ConfigFactory.load()


    // First we will be needing a spark context
    val conf = new SparkConf().setAppName(config.getString("application.name"))
    val ssc = new StreamingContext(conf, Seconds(config.getInt("application.batchIntervalInSeconds")))

    val kafkaParams = KafkaProp.getKafkaParams()
    val topics = KafkaProp.getTopics()

    val inStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val enrichedStream = inStream.map(_.value())
      .map(x => JsonParser(x).convertTo[calls] )
      .map(calls => ProcessCalls.enrichCalls(calls))

    val windowedStream = enrichedStream.window(Seconds(60), Seconds(10))

    val outStream=windowedStream.map(calls_enriched => (calls_enriched.total_call_duration, calls_enriched.total_queue_duration,calls_enriched.total_handling_duration, 1))
        .reduce({
          case ((total_call_duration1: Long, total_queue_duration1 : Long, total_handling_duration1: Long, total_calls1: Int ),(total_call_duration2 :Long, total_queue_duration2 : Long, total_handling_duration2: Long, total_calls2: Int)) =>
            (total_call_duration1+total_call_duration2, total_queue_duration1 + total_queue_duration2, total_handling_duration1+total_handling_duration2, total_calls1+total_calls2)
        })
        .map(record => (record._1/record._4, record._2/record._4, record._3/record._4, record._4))


    enrichedStream.print(2)
    outStream.print(10)

    ssc.start()
    ssc.awaitTermination()
  }


}

