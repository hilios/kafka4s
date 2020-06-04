package io.kafka4s.fs2.consumer

import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import fs2.concurrent.SignallingRef
import fs2.{Chunk, Stream}
import io.kafka4s.consumer._
import io.kafka4s.effect.consumer._
import io.kafka4s.effect.consumer.config._
import io.kafka4s.effect.log._
import io.kafka4s.effect.log.slf4j.Slf4jLogger
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.concurrent.duration._
import scala.util.Random

class Fs2KafkaConsumer[F[_]] private (config: KafkaConsumerConfiguration,
                                      consumer: ConsumerEffect[F],
                                      logger: Logger[F],
                                      maxConcurrent: Int,
                                      pollTimeout: FiniteDuration,
                                      subscription: Subscription,
                                      recordConsumer: RecordConsumer[F])(implicit F: Concurrent[F], T: Timer[F]) {

  private val maxAttempts = 10
  private val maxDelay    = pollTimeout

  private def commit(records: Chunk[ConsumerRecord[F]]): F[Unit] =
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
      r <- Stream
        .fromIterator(p)
        .parEvalMap(maxConcurrent)(consume1)
        .filter(_.isInstanceOf[Return.Ack[F]])
        .map(_.record)
        .chunks
      _ <- Stream.eval(commit(r))
    } yield ()

  private def subscribe: F[Unit] =
    subscription match {
      case Subscription.Topics(topics) => consumer.subscribe(topics.toSeq)
      case Subscription.Pattern(regex) => consumer.subscribe(regex)
      case Subscription.Empty          => F.unit
    }

  private def close: F[Unit] = logger.debug("Stopping Kafka consumer") >> consumer.wakeup

  def stream: Stream[F, Unit] =
    for {
      _ <- Stream.eval(logger.info(
        s"Fs2KafkaConsumer connecting to [${config.bootstrapServers.mkString(",")}] with group id [${config.groupId}]"))
      _          <- Stream.eval(subscribe)
      exitSignal <- Stream.eval(SignallingRef[F, Boolean](false))
      _          <- fetch.repeat.interruptWhen(exitSignal).onFinalize(close)
    } yield ()
}

object Fs2KafkaConsumer {

  def apply[F[_]](builder: Fs2KafkaConsumerBuilder[F])(implicit F: ConcurrentEffect[F],
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
      logger   <- Stream.eval(Slf4jLogger[F].ofT[Fs2KafkaConsumer])
      c = new Fs2KafkaConsumer[F](config,
                                  consumer,
                                  logger,
                                  builder.maxConcurrent,
                                  builder.pollTimeout,
                                  builder.subscription,
                                  builder.recordConsumer)
      _ <- c.stream
    } yield ()
}
