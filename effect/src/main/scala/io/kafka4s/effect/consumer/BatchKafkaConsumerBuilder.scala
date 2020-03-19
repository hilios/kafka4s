package io.kafka4s.effect.consumer

import java.util.Properties

import cats.ApplicativeError
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import io.kafka4s.consumer._
import io.kafka4s.effect.config

import scala.concurrent.duration._
import scala.util.matching.Regex

case class BatchKafkaConsumerBuilder[F[_]](pollTimeout: FiniteDuration,
                                           properties: Properties,
                                           subscription: Subscription,
                                           recordConsumer: BatchRecordConsumer[F]) {

  type Self = BatchKafkaConsumerBuilder[F]

  def withTopics(topics: String*): Self =
    copy(subscription = Subscription.Topics(topics.toSet))

  def withTopics(topics: Set[String]): Self =
    copy(subscription = Subscription.Topics(topics))

  def withProperties(properties: Properties): Self =
    copy(properties = properties)

  def withProperties(properties: Map[String, String]): Self =
    copy(properties = config.mapToProperties(properties))

  def withPattern(regex: Regex): Self =
    copy(subscription = Subscription.Pattern(regex))

  def withPollTimeout(duration: FiniteDuration): Self =
    copy(pollTimeout = duration)

  def withConsumer(consumer: BatchConsumer[F])(implicit F: ApplicativeError[F, Throwable]): Self =
    copy(recordConsumer = consumer.orNotFound)

  def withConsumer(consumer: BatchRecordConsumer[F]): Self =
    copy(recordConsumer = consumer)

  def resource(implicit F: ConcurrentEffect[F], T: Timer[F], CS: ContextShift[F]): Resource[F, BatchKafkaConsumer[F]] =
    BatchKafkaConsumer.resource[F](builder = this)

  def serve(implicit F: ConcurrentEffect[F], T: Timer[F], CS: ContextShift[F]): F[Unit] = resource.use(_ => F.never)
}

object BatchKafkaConsumerBuilder {

  def apply[F[_]: Sync]: BatchKafkaConsumerBuilder[F] =
    BatchKafkaConsumerBuilder[F](pollTimeout    = 100.millis,
                                 properties     = new Properties(),
                                 subscription   = Subscription.Empty,
                                 recordConsumer = BatchConsumer.empty[F].orNotFound)
}
