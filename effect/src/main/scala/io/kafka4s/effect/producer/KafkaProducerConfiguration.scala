package io.kafka4s.effect.producer

import java.util.Properties

import cats.syntax.either._
import io.kafka4s.effect.config._
import io.kafka4s.effect.producer.adts.{Acks, CompressionType}
import org.apache.kafka.clients.producer.ProducerConfig

case class KafkaProducerConfiguration private (bootstrapServers: Seq[String],
                                               compression: CompressionType,
                                               acks: Acks,
                                               enableIdempotent: Boolean,
                                               properties: Properties)

object KafkaProducerConfiguration {

  def load: Either[Throwable, KafkaProducerConfiguration] =
    for {
      properties <- configToProperties("kafka4s.producer")
      config     <- loadFrom(properties)
    } yield config

  def loadFrom(properties: Properties): Either[Throwable, KafkaProducerConfiguration] =
    for {
      bootstrapServers <- properties.getter[String](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
      compressionType <- properties.getter[Option[String]](ProducerConfig.COMPRESSION_TYPE_CONFIG) flatMap { value =>
        Either.catchNonFatal(value.map(CompressionType(_)).getOrElse(CompressionType.None))
      }
      acks <- properties.getter[Option[String]](ProducerConfig.ACKS_CONFIG) flatMap { value =>
        Either.catchNonFatal(value.map(Acks(_)).getOrElse(Acks.One))
      }
      enableIdempotent <- properties.getter[Option[Boolean]](ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG) map { value =>
        value.getOrElse(false)
      }
    } yield
      new KafkaProducerConfiguration(bootstrapServers.split(raw",").map(_.trim),
                                     compressionType,
                                     acks,
                                     enableIdempotent,
                                     properties)
}
