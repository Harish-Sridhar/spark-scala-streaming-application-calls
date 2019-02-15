package com.hs.spark.streaming.calls

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.JavaConversions.asScalaIterator

object KafkaProp {

  val config: Config = ConfigFactory.load()

  def getKafkaParams(): kafkaParams = {
    Map[String, Object](
      "bootstrap.servers" -> config.getString("kafka.bootstrapServers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> config.getString("kafka.groupId"),
      "auto.offset.reset" -> config.getString("kafka.autoOffsetReset")
    )
  }

  def getTopics(): Array[String] = {
    config.getStringList("kafka.topics").toArray.map(_.toString)
  }

}
