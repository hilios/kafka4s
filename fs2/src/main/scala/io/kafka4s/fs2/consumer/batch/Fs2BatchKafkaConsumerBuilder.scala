package io.kafka4s.fs2.consumer.batch

import java.util.Properties

import cats.ApplicativeError
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import fs2.Stream
import io.kafka4s.consumer.Subscription
import io.kafka4s.consumer.batch._
import io.kafka4s.effect.properties.implicits._

import scala.concurrent.duration._
import scala.util.matching.Regex

case class Fs2BatchKafkaConsumerBuilder[F[_]](blocker: Blocker,
                                              maxConcurrent: Int,
                                              pollTimeout: FiniteDuration,
                                              properties: Properties,
                                              subscription: Subscription,
                                              recordConsumer: BatchRecordConsumer[F]) {

  type Self = Fs2BatchKafkaConsumerBuilder[F]

  def withTopics(topics: String*): Self =
    copy(subscription = Subscription.Topics(topics.toSet))

  def withTopics(topics: Set[String]): Self =
    copy(subscription = Subscription.Topics(topics))

  def withProperties(properties: Properties): Self =
    copy(properties = properties)

  def withProperties(properties: Map[String, String]): Self =
    copy(properties = properties.toProperties)

  def withPattern(regex: Regex): Self =
    copy(subscription = Subscription.Pattern(regex))

  def withPollTimeout(duration: FiniteDuration): Self =
    copy(pollTimeout = duration)

  def withConsumer(consumer: BatchConsumer[F])(implicit F: ApplicativeError[F, Throwable]): Self =
    copy(recordConsumer = consumer.orNotFound)

  def withConsumer(consumer: BatchRecordConsumer[F]): Self =
    copy(recordConsumer = consumer)

  def withMaxConcurrency(maxConcurrent: Int) =
    copy(maxConcurrent = maxConcurrent)

  def stream(implicit F: ConcurrentEffect[F], T: Timer[F], CS: ContextShift[F]): Stream[F, Unit] =
    Fs2BatchKafkaConsumer[F](builder = this)

  def resource(implicit F: ConcurrentEffect[F], T: Timer[F], CS: ContextShift[F]): Resource[F, Unit] =
    stream.compile.resource.drain

  def serve(implicit F: ConcurrentEffect[F], T: Timer[F], CS: ContextShift[F]): F[Unit] =
    stream.compile.drain
}

object Fs2BatchKafkaConsumerBuilder {

  def apply[F[_]: Sync](blocker: Blocker): Fs2BatchKafkaConsumerBuilder[F] =
    Fs2BatchKafkaConsumerBuilder[F](
      blocker,
      maxConcurrent  = 1,
      pollTimeout    = 100.millis,
      properties     = new Properties(),
      subscription   = Subscription.Empty,
      recordConsumer = BatchConsumer.empty[F].orNotFound
    )
}
