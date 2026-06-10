package io.kafka4s.effect.admin

import cats.effect.{Async, Concurrent, Resource}
import io.kafka4s.effect.admin.config._
import io.kafka4s.effect.properties.implicits._

import java.util.Properties

case class KafkaAdminBuilder[F[_]] private (properties: Properties) {

  type Self = KafkaAdminBuilder[F]

  def withProperties(properties: Properties): Self =
    copy(properties = properties)

  def withProperties(properties: Map[String, String]): Self =
    copy(properties = properties.toProperties)

  def resource(implicit F: Async[F]): Resource[F, AdminEffect[F]] =
    for {
      config <- Resource.eval(F.fromEither {
        if (properties.isEmpty) KafkaAdminConfiguration.load else KafkaAdminConfiguration.loadFrom(properties)
      })
      admin <- Resource.make(AdminEffect[F](config.properties))(_.close())
    } yield admin
}

object KafkaAdminBuilder {

  def apply[F[_]: Concurrent]: KafkaAdminBuilder[F] =
    KafkaAdminBuilder[F](properties = new Properties())
}
