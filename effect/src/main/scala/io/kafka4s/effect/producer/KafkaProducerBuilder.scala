package io.kafka4s.effect.producer

import cats.effect.Concurrent
import cats.effect.Resource
import io.kafka4s.effect.properties.implicits._

import java.util.Properties

case class KafkaProducerBuilder[F[_]](properties: Properties) {

  type Self = KafkaProducerBuilder[F]

  def withProperties(properties: Properties): Self =
    copy(properties = properties)

  def withProperties(properties: Map[String, String]): Self =
    copy(properties = properties.toProperties)

  def resource(implicit F: Concurrent[F]): Resource[F, KafkaProducer[F]] =
    KafkaProducer.resource[F](builder = this)
}

object KafkaProducerBuilder {
  def apply[F[_]]: KafkaProducerBuilder[F] = KafkaProducerBuilder(properties = new Properties())
}
