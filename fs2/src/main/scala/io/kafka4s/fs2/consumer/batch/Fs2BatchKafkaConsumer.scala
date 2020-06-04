package io.kafka4s.fs2.consumer.batch

import cats.data.NonEmptyList
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.kafka4s.BatchRecordConsumer
import io.kafka4s.consumer._
import io.kafka4s.consumer.batch.BatchReturn
import io.kafka4s.effect.consumer._
import io.kafka4s.effect.consumer.config._
import io.kafka4s.effect.log._
import io.kafka4s.effect.log.slf4j.Slf4jLogger
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.concurrent.duration._
import scala.util.Random

class Fs2BatchKafkaConsumer[F[_]] private (
  config: KafkaConsumerConfiguration,
  consumer: ConsumerEffect[F],
  logger: Logger[F],
  maxConcurrent: Int,
  pollTimeout: FiniteDuration,
  subscription: Subscription,
  batchConsumer: BatchRecordConsumer[F]
)(implicit F: Concurrent[F], T: Timer[F]) {

  private val maxAttempts = 10
  private val maxDelay    = pollTimeout

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

  private def consumeBatch(records: NonEmptyList[DefaultConsumerRecord]): F[BatchReturn[F]] =
    for {
      r <- batchConsumer.apply(records.map(ConsumerRecord[F]))
      _ <- r match {
        case BatchReturn.Ack(r)     => logger.debug(s"Records [${r.mkString_(",")}] processed successfully")
        case BatchReturn.Err(r, ex) => logger.error(s"Error processing [${r.mkString_(",")}]", ex)
      }
    } yield r

  private def fullJitterDelay(delay: FiniteDuration): FiniteDuration = {
    val e = Math.pow(2L, delay.toNanos.toDouble) * Random.nextDouble()
    FiniteDuration(e.toLong, NANOSECONDS)
  }

  private def fetch: Stream[F, Unit] =
    for {
      _ <- Stream.eval(logger.trace("Polling records..."))
      p <- Stream.retry(
        consumer.poll(pollTimeout),
        delay       = maxDelay,
        nextDelay   = fullJitterDelay,
        maxAttempts = maxAttempts,
        _.isInstanceOf[KafkaException]
      )
      b <- Stream
        .fromIterator(p)
        .groupAdjacentBy(_.topic())
        .map(_._2.toNel)
        .collect {
          case Some(i) => i
        }
        .parEvalMap(maxConcurrent)(consumeBatch)
        .filter(_.isInstanceOf[BatchReturn.Ack[F]])
        .map(_.records)
      _ <- Stream.eval(commit(b))
    } yield ()

  private def subscribe: F[Unit] =
    subscription match {
      case Subscription.Topics(topics) => consumer.subscribe(topics.toSeq)
      case Subscription.Pattern(regex) => consumer.subscribe(regex)
      case Subscription.Empty          => F.unit
    }

  private def close: F[Unit] = logger.debug("Stopping Kafka batch consumer") >> consumer.wakeup

  def stream: Stream[F, Unit] =
    for {
      _ <- Stream.eval(logger.info(
        s"Fs2BatchKafkaConsumer connecting to [${config.bootstrapServers.mkString(",")}] with group id [${config.groupId}]"))
      _          <- Stream.eval(subscribe)
      exitSignal <- Stream.eval(SignallingRef[F, Boolean](false))
      _          <- fetch.repeat.interruptWhen(exitSignal).onFinalize(close)
    } yield ()
}

object Fs2BatchKafkaConsumer {

  def apply[F[_]](builder: Fs2BatchKafkaConsumerBuilder[F])(implicit F: ConcurrentEffect[F],
                                                            T: Timer[F],
                                                            CS: ContextShift[F]): Stream[F, Unit] =
    for {
      config <- Stream.eval(F.fromEither {
        if (builder.properties.isEmpty) KafkaConsumerConfiguration.load
        else KafkaConsumerConfiguration.loadFrom(builder.properties)
      })
      properties <- Stream.eval(F.delay {
        val p = config.properties
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        p
      })
      consumer <- Stream.resource(ConsumerEffect.resource[F](properties, builder.blocker))
      logger   <- Stream.eval(Slf4jLogger[F].ofT[Fs2BatchKafkaConsumer])
      c = new Fs2BatchKafkaConsumer[F](config,
                                       consumer,
                                       logger,
                                       builder.maxConcurrent,
                                       builder.pollTimeout,
                                       builder.subscription,
                                       builder.recordConsumer)
      _ <- c.stream
    } yield ()

}
