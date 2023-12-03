package io.kafka4s.effect.consumer.config

import cats.syntax.either._
import io.kafka4s.effect.properties
import io.kafka4s.effect.properties.implicits._
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

case class KafkaConsumerConfiguration private (bootstrapServers: Seq[String],
                                               groupId: String,
                                               autoOffsetReset: AutoOffsetReset,
                                               properties: Properties)

object KafkaConsumerConfiguration {

  def load: Either[Throwable, KafkaConsumerConfiguration] =
    for {
      properties <- properties.fromConfig("kafka4s.consumer")
      config     <- loadFrom(properties)
    } yield config

  def loadFrom(properties: Properties): Either[Throwable, KafkaConsumerConfiguration] =
    for {
      bootstrapServers <- properties.getter[String](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
      groupId          <- properties.getter[String](ConsumerConfig.GROUP_ID_CONFIG)
      autoOffsetReset  <- properties.getter[Option[String]](ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).flatMap { value =>
        Either.catchNonFatal(value.map(AutoOffsetReset(_)).getOrElse(AutoOffsetReset.Latest))
      }
    } yield KafkaConsumerConfiguration(bootstrapServers.split(",").map(_.trim), groupId, autoOffsetReset, properties)
}
