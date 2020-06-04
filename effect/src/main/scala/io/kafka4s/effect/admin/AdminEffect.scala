package io.kafka4s.effect.admin

import java.time.{Duration => JDuration}
import java.util.Properties
import java.util.concurrent.{Future => JFuture}

import cats.effect.{Concurrent, Timer}
import cats.implicits._
import io.kafka4s.effect.log.Logger
import io.kafka4s.effect.log.slf4j.Slf4jLogger
import io.kafka4s.effect.utils.Await
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.KafkaFuture

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class AdminEffect[F[_]] private (admin: AdminClient, logger: Logger[F], timeout: FiniteDuration = 30.seconds)(
  implicit F: Concurrent[F],
  A: Await[F, JFuture]) {

  def createTopics(newTopics: Seq[NewTopic]): F[Unit] =
    logger.debug(s"Creating topics: ${newTopics.map(_.name()).mkString(", ")}") >>
      liftF { _ =>
        F.delay(admin.createTopics(newTopics.asJavaCollection).all())
      }.void

  def deleteTopics(topics: Seq[String]): F[Unit] =
    logger.debug(s"Deleting topics: ${topics.mkString(", ")}") >>
      liftF { _ =>
        F.delay(admin.deleteTopics(topics.asJavaCollection).all())
      }.void

  def liftF[A](fn: AdminClient => F[KafkaFuture[A]]): F[A] =
    for {
      f <- fn(admin)
      a <- A.await(timeout)(f)
    } yield a

  def close(timeout: FiniteDuration = 30.seconds): F[Unit] =
    F.delay(admin.close(JDuration.ofMillis(timeout.toMillis)))
}

object AdminEffect {

  def apply[F[_]](properties: Properties)(implicit F: Concurrent[F], T: Timer[F]): F[AdminEffect[F]] =
    for {
      logger <- Slf4jLogger[F].ofT[AdminEffect]
      admin  <- F.delay(AdminClient.create(properties))
    } yield new AdminEffect[F](admin, logger)
}
