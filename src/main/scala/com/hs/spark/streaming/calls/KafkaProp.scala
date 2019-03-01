package com.hs.spark.streaming.calls

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConversions.asScalaIterator

object KafkaProp {

  val config: Config = ConfigFactory.load()

  def getKafkaConsumerParams(): kafkaParams = {
    Map[String, Object](
      "bootstrap.servers" -> config.getString("kafka.bootstrapServers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> config.getString("kafka.groupId"),
      "auto.offset.reset" -> config.getString("kafka.autoOffsetReset")
    )
  }

  def getKafkaProducerParams(): kafkaParams = {
    Map[String, Object](
      "bootstrap.servers" -> config.getString("kafka.bootstrapServers"),
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer]
    )
  }

  def getInTopics(): Array[String] = {
    config.getStringList("kafka.inTopics").toArray.map(_.toString)
  }

  def getOutTopics(): Array[String] = {
    config.getStringList("kafka.outTopics").toArray.map(_.toString)
  }
}
