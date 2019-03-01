package com.hs.spark.streaming.calls

import com.typesafe.config.{Config, ConfigFactory}
import MyJsonProtocol._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.joda.time.DateTime
import spray.json._

object Application {

  def main(args: Array[String]): Unit = {


    // Loading configurations needed.
    val config: Config = ConfigFactory.load()


    // First we will be needing a spark context
    val conf = new SparkConf().setAppName(config.getString("application.name"))
    val ssc = new StreamingContext(conf, Seconds(config.getInt("application.batchIntervalInSeconds")))

    val kafkaConsumerParams = KafkaProp.getKafkaConsumerParams()
    val inTopics = KafkaProp.getInTopics()

    // Create an incoming stream from kafka topic
    val inStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](inTopics, kafkaConsumerParams)
    )

    // enrich the incoming data
    val enrichedStream = inStream.map(_.value())
      .map(x => JsonParser(x).convertTo[calls] )
      .map(calls => ProcessCalls.enrichCalls(calls))


    // Create windows on the stream
    val windowedStream = enrichedStream.window(Seconds(60), Seconds(10))

    // group by calls based on customer and find repeat customer
    val customerAggStream = windowedStream.transform(rdd => ProcessCalls.calculateRepeatCustomer(rdd))


    // calculate metrics
    val metricsStream=customerAggStream.map(set => set._2)
      .transform((rdd, time) => ProcessCalls.calculateMetrics(rdd,time))

    // group by call reasons  and find top 5 call reasons
    val top5CallReasonsStream = windowedStream.transform((rdd,time) => ProcessCalls.calculateTop5CallReasons(rdd,time))



    // create a spark producer broadcast variable
    val kafkaProducerParams = KafkaProp.getKafkaProducerParams()

    val kafkaSink = ssc.sparkContext.broadcast(KafkaSink(kafkaProducerParams))

    val outTopics = KafkaProp.getOutTopics()

    enrichedStream.foreachRDD{ rdd =>
      rdd.foreach { message =>
        kafkaSink.value.send(outTopics(0), message.call_id.toString, message.toJson.toString())
      }
    }

    metricsStream.foreachRDD{ rdd =>
      rdd.foreach { message =>
        kafkaSink.value.send(outTopics(1), message.toJson.toString())
      }
    }

    top5CallReasonsStream.foreachRDD{
      rdd => rdd.foreach{
        message =>
          kafkaSink.value.send(outTopics(2),message.toJson.toString())
      }
    }


    enrichedStream.print(2)
    metricsStream.print(10)
    top5CallReasonsStream.print(5)

    ssc.start()
    ssc.awaitTermination()
  }


}

