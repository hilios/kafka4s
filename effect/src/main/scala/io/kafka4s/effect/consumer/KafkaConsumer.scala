package io.kafka4s.effect.consumer

import cats.data.NonEmptyList
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import io.kafka4s.RecordConsumer
import io.kafka4s.consumer.{ConsumerRecord, DefaultConsumerRecord, Return, Subscription}
import io.kafka4s.effect.consumer.config.KafkaConsumerConfiguration
import io.kafka4s.effect.log.Logger
import io.kafka4s.effect.log.slf4j.Slf4jLogger
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.concurrent.duration.FiniteDuration

class KafkaConsumer[F[_]](config: KafkaConsumerConfiguration,
                          consumer: ConsumerEffect[F],
                          logger: Logger[F],
                          pollTimeout: FiniteDuration,
                          subscription: Subscription,
                          recordConsumer: RecordConsumer[F])(implicit F: Concurrent[F], T: Timer[F]) {

  // TODO: Parametrize the retry policy
  val retryPolicy: retry.RetryPolicy[F] =
    retry.RetryPolicies.limitRetries[F](maxRetries = 10) join retry.RetryPolicies.fullJitter(pollTimeout)

  private def commit(records: NonEmptyList[ConsumerRecord[F]]): F[Unit] =
    for {
      o <- F.pure(records.toList.map { record =>
        new TopicPartition(record.topic, record.partition) -> new OffsetAndMetadata(record.offset + 1L)
      })
      c <- consumer.commit(o.toMap).attempt
      _ <- c.fold(
        e => logger.error(s"Error committing offsets for records [${records.mkString_(", ")}]", e),
        _ => logger.debug(s"Offset committed for records [${records.mkString_(", ")}]")
      )
    } yield ()

  private def consume1(record: DefaultConsumerRecord): F[Return[F]] =
    for {
      r <- recordConsumer.apply(ConsumerRecord[F](record))
      _ <- r match {
        case Return.Ack(r)     => logger.debug(s"Record [${r.show}] processed successfully")
        case Return.Err(r, ex) => logger.error(s"Error processing [${r.show}]", ex)
      }
    } yield r

  private def consume(records: NonEmptyList[DefaultConsumerRecord]): F[Unit] =
    for {
      r <- records.traverse(consume1)
      _ <- r.filter(_.isInstanceOf[Return.Ack[F]]).map(_.record) match {
        case Nil => F.unit
        case a   => F.delay(NonEmptyList.fromListUnsafe(a)) >>= commit
      }
    } yield ()

  private def logErrors(throwable: Throwable, details: retry.RetryDetails): F[Unit] =
    details match {
      case retry.RetryDetails.WillDelayAndRetry(nextDelay, retriesSoFar, _) =>
        logger.warn(s"Consumer failed unexpectedly $retriesSoFar time(s). Retrying in $nextDelay", throwable)
      case retry.RetryDetails.GivingUp(totalRetries, totalDelay) =>
        logger.error(s"Consumer failed after $totalDelay and $totalRetries retries", throwable)
    }

  private val onKafkaException: Throwable => Boolean = (_: Throwable).isInstanceOf[KafkaException]

  private def fetch(exitSignal: Ref[F, Boolean]): F[Unit] = {
    val loop = for {
      _       <- logger.trace("Polling records...")
      records <- consumer.poll(pollTimeout)
      _       <- NonEmptyList.fromList(records.toList).fold(logger.trace("No records fetched"))(consume)
      exit    <- exitSignal.get
    } yield exit

    retry
      .retryingOnSomeErrors(retryPolicy, onKafkaException, logErrors)(loop)
      .flatMap(exit => if (exit) F.unit else fetch(exitSignal))
  }

  private def subscribe: F[Unit] = subscription match {
    case Subscription.Topics(topics) => consumer.subscribe(topics.toSeq)
    case Subscription.Pattern(regex) => consumer.subscribe(regex)
    case Subscription.Empty          => F.unit
  }

  private def start: F[CancelToken[F]] =
    for {
      exitSignal <- Ref.of[F, Boolean](false)
      _ <- logger.info(
        s"KafkaConsumer connecting to [${config.bootstrapServers.mkString(",")}] with group id [${config.groupId}]")
      _     <- subscribe
      fiber <- F.start(fetch(exitSignal))
    } yield exitSignal.set(true) >> fiber.join

  private def close: F[Unit] =
    logger.debug("Stopping Kafka consumer")

  def resource: Resource[F, Unit] =
    Resource.make(start)(close >> _).void
}

object KafkaConsumer {

  def resource[F[_]](builder: KafkaConsumerBuilder[F])(implicit F: ConcurrentEffect[F],
                                                       T: Timer[F],
                                                       CS: ContextShift[F]): Resource[F, KafkaConsumer[F]] =
    for {
      config <- Resource.liftF(F.fromEither {
        if (builder.properties.isEmpty) KafkaConsumerConfiguration.load
        else KafkaConsumerConfiguration.loadFrom(builder.properties)
      })
      properties <- Resource.liftF(F.delay {
        val p = config.properties
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        p
      })
      consumer <- ConsumerEffect.resource[F](properties, builder.blocker)
      logger   <- Resource.liftF(Slf4jLogger[F].ofT[KafkaConsumer])
      c = new KafkaConsumer[F](config,
                               consumer,
                               logger,
                               builder.pollTimeout,
                               builder.subscription,
                               builder.recordConsumer)
      _ <- c.resource
    } yield c
}
