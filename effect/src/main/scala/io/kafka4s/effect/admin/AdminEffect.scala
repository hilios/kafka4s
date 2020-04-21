package io.kafka4s.effect.admin

import java.time.{Duration => JDuration}
import java.util.Properties
import java.util.concurrent.{Future => JFuture}

import cats.effect.{Concurrent, Timer}
import cats.implicits._
import io.kafka4s.effect.utils.Await
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class AdminEffect[F[_]] private (admin: AdminClient, timeout: FiniteDuration = 30.seconds)(implicit F: Concurrent[F],
                                                                                           A: Await[F, JFuture]) {

  def createTopics(newTopics: Seq[NewTopic]): F[Unit] =
    for {
      future <- F.delay(admin.createTopics(newTopics.asJavaCollection).all())
      _      <- A.await(timeout)(future)
    } yield ()

  def deleteTopics(topics: Seq[String]): F[Unit] =
    for {
      future <- F.delay(admin.deleteTopics(topics.asJavaCollection).all())
      _      <- A.await(timeout)(future)
    } yield ()

  def close(timeout: FiniteDuration = 30.seconds): F[Unit] =
    F.delay(admin.close(JDuration.ofMillis(timeout.toMillis)))
}

object AdminEffect {

  def apply[F[_]](properties: Properties)(implicit F: Concurrent[F], T: Timer[F]): F[AdminEffect[F]] =
    for {
      admin <- F.delay(AdminClient.create(properties))
    } yield new AdminEffect[F](admin)
}
