package io.kafka4s.effect.consumer

import java.util.concurrent.Executors

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{Blocker, CancelToken, Concurrent, ConcurrentEffect, ContextShift, Resource, Timer}
import cats.implicits._
import io.kafka4s.BatchRecordConsumer
import io.kafka4s.consumer.{BatchReturn, ConsumerRecord, DefaultConsumerRecord, Subscription}
import io.kafka4s.effect.log.Logger
import io.kafka4s.effect.log.impl.Slf4jLogger
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.concurrent.duration.FiniteDuration

class BatchKafkaConsumer[F[_]](config: KafkaConsumerConfiguration,
                               consumer: ConsumerEffect[F],
                               logger: Logger[F],
                               pollTimeout: FiniteDuration,
                               subscription: Subscription,
                               batchConsumer: BatchRecordConsumer[F])(implicit F: Concurrent[F], T: Timer[F]) {

  // TODO: Parametrize the retry policy
  val retryPolicy: retry.RetryPolicy[F] =
    retry.RetryPolicies.limitRetries[F](maxRetries = 10) join retry.RetryPolicies.fullJitter(pollTimeout)

  private def commit(records: NonEmptyList[ConsumerRecord[F]]): F[Unit] =
    for {
      offsets <- F.pure(records.toList.map { record =>
        new TopicPartition(record.topic, record.partition) -> new OffsetAndMetadata(record.offset + 1L)
      })
      _ <- consumer.commit(offsets.toMap)
      _ <- logger.debug(s"Offset committed for records [${records.mkString_(", ")}]")
    } yield ()

  private def consumeBatch(records: NonEmptyList[DefaultConsumerRecord]): F[BatchReturn[F]] =
    for {
      r <- batchConsumer.apply(records.map(ConsumerRecord[F]))
      _ <- r match {
        case BatchReturn.Ack(r)     => logger.debug(s"Records [${r.show}] processed successfully")
        case BatchReturn.Err(r, ex) => logger.error(s"Error processing [${r.show}]", ex)
      }
    } yield r

  private def consume(records: NonEmptyList[DefaultConsumerRecord]): F[Unit] =
    for {
      b <- F.pure(records.groupBy(_.topic()))
      r <- b.mapValues(consumeBatch).values.toList.sequence
      _ <- r.filter(_.isInstanceOf[BatchReturn.Ack[F]]).flatMap(_.records.toList) match {
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

  private def onKafkaExceptions(throwable: Throwable): Boolean = throwable match {
    case _: KafkaException => true
    case _                 => false
  }

  private def fetch(exitSignal: Ref[F, Boolean]): F[Unit] = {
    val loop = for {
      _       <- logger.trace("Polling records...")
      records <- consumer.poll(pollTimeout)
      _       <- if (records.isEmpty) F.unit else F.delay(NonEmptyList.fromListUnsafe(records.toList)) >>= consume
      exit    <- exitSignal.get
    } yield exit

    retry.retryingOnSomeErrors(retryPolicy, onKafkaExceptions, logErrors) {
      loop.flatMap(exit => if (exit) F.unit else fetch(exitSignal))
    }
  }

  private def subscribe: F[Unit] = subscription match {
    case Subscription.Topics(topics) => consumer.subscribe(topics.toSeq)
    case Subscription.Pattern(regex) => consumer.subscribe(regex)
    case Subscription.Empty          => F.unit
  }

  def start: F[CancelToken[F]] =
    for {
      exitSignal <- Ref.of[F, Boolean](false)
      _ <- logger.info(
        s"BatchKafkaConsumer connecting to [${config.bootstrapServers.mkString(",")}] with group id [${config.groupId}]")
      _     <- subscribe
      fiber <- F.start(fetch(exitSignal))
    } yield exitSignal.set(true) >> fiber.join

  def close: F[Unit] =
    logger.info("Stopping BatchKafkaConsumer...")

  def resource: Resource[F, Unit] =
    Resource.make(start)(close >> _).void
}

object BatchKafkaConsumer {

  def resource[F[_]](builder: BatchKafkaConsumerBuilder[F])(implicit F: ConcurrentEffect[F],
                                                            T: Timer[F],
                                                            CS: ContextShift[F]): Resource[F, BatchKafkaConsumer[F]] =
    for {
      config <- Resource.liftF(F.fromEither {
        if (builder.properties.isEmpty) KafkaConsumerConfiguration.load
        else KafkaConsumerConfiguration.loadFrom(builder.properties)
      })
      properties <- Resource.liftF(F.delay {
        val p = config.properties
        p.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        p
      })
      es <- Resource.make(F.delay(Executors.newCachedThreadPool()))(e => F.delay(e.shutdown()))
      blocker = Blocker.liftExecutorService(es)
      consumer <- Resource.make(ConsumerEffect[F](properties, blocker))(c => c.wakeup >> c.close())
      logger   <- Resource.liftF(Slf4jLogger[F].of[BatchKafkaConsumer[Any]])
      c = new BatchKafkaConsumer[F](config,
                                    consumer,
                                    logger,
                                    builder.pollTimeout,
                                    builder.subscription,
                                    builder.recordConsumer)
      _ <- c.resource
    } yield c
}
