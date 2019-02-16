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

    val outStream = inStream.map(_.value())
      .map(x => JsonParser(x).convertTo[calls] )
      .map(calls => CallsProcess.calculateTimeUnits(calls))

    outStream.print(10)

    ssc.start()
    ssc.awaitTermination()
  }


}

