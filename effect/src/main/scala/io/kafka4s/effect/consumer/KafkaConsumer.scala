package io.kafka4s.effect.consumer

import java.util.concurrent.Executors

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import io.kafka4s.RecordConsumer
import io.kafka4s.consumer.{ConsumerRecord, DefaultConsumerRecord, Return, Subscription}
import io.kafka4s.effect.BANNER
import io.kafka4s.effect.log.Log
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration.FiniteDuration

class KafkaConsumer[F[_]](config: KafkaConsumerConfiguration,
                          pollTimeout: FiniteDuration,
                          subscription: Subscription,
                          consumer: ConsumerEffect[F],
                          recordConsumer: RecordConsumer[F])(implicit F: Concurrent[F], L: Log[F]) {

  private def commit(records: Seq[ConsumerRecord[F]]): F[Unit] =
    if (records.isEmpty) F.unit
    else
      for {
        offsets <- F.pure(records.toList.map { record =>
          new TopicPartition(record.topic, record.partition) -> new OffsetAndMetadata(record.offset + 1L)
        })
        _ <- consumer.commit(offsets.toMap)
        _ <- L.debug(s"Committing records [${records.map(_.show).mkString(", ")}]")
      } yield ()

  private def consume1(record: DefaultConsumerRecord): F[Return[F]] =
    for {
      r <- recordConsumer.apply(ConsumerRecord[F](record))
      _ <- r match {
        case Return.Ack(r)     => L.debug(s"Record [${r.show}] processed successfully")
        case Return.Err(r, ex) => L.error(s"Error processing [${r.show}]", ex)
      }
    } yield r

  private def consume(records: Iterable[DefaultConsumerRecord]): F[Unit] =
    for {
      r <- records.toVector.traverse(consume1)
      a = r
        .filter {
          case Return.Ack(_) => true
          case _             => false
        }
        .map(_.record)
      _ <- commit(a)
    } yield ()

  private def fetch(exitSignal: Ref[F, Boolean]): F[Unit] = {
    val loop = for {
      records <- consumer.poll(pollTimeout)
      _       <- if (records.isEmpty) F.unit else consume(records)
      exit    <- exitSignal.get
    } yield exit

    loop.flatMap(exit => if (exit) F.unit else fetch(exitSignal))
  }

  private def subscribe: F[Unit] = subscription match {
    case Subscription.Topics(topics) => consumer.subscribe(topics.toSeq)
    case Subscription.Pattern(regex) => consumer.subscribe(regex)
    case Subscription.Empty          => F.unit
  }

  def start: F[CancelToken[F]] =
    for {
      exitSignal <- Ref.of[F, Boolean](false)
      _          <- L.info(BANNER)
      _          <- L.info(s"Kafka connecting to [${config.bootstrapServers}]")
      _          <- subscribe
      fiber      <- F.start(fetch(exitSignal))
    } yield exitSignal.set(true) >> fiber.join

  def resource: Resource[F, Unit] =
    Resource.make(start)(identity).void
}

object KafkaConsumer {

  def resource[F[_]](builder: KafkaConsumerBuilder[F])(implicit F: Concurrent[F],
                                                       CS: ContextShift[F]): Resource[F, KafkaConsumer[F]] =
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
      consumer <- Resource.make(ConsumerEffect[F](properties, blocker))(_.close())
      c = new KafkaConsumer[F](config, builder.pollTimeout, builder.subscription, consumer, builder.recordConsumer)
      _ <- c.resource
    } yield c
}
