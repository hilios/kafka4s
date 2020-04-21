package io.kafka4s.effect.admin.config

import java.util.Properties

import io.kafka4s.effect.properties
import io.kafka4s.effect.properties.implicits._
import org.apache.kafka.clients.consumer.ConsumerConfig

case class KafkaAdminConfiguration(bootstrapServers: Seq[String], properties: Properties)

object KafkaAdminConfiguration {

  def load: Either[Throwable, KafkaAdminConfiguration] =
    for {
      properties <- properties.fromConfig("kafka4s.consumer")
      config     <- loadFrom(properties)
    } yield config

  def loadFrom(properties: Properties): Either[Throwable, KafkaAdminConfiguration] =
    for {
      bootstrapServers <- properties.getter[String](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
    } yield KafkaAdminConfiguration(bootstrapServers.split(raw",").map(_.trim), properties)
}
