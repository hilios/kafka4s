package io.kafka4s.middlewares.dlq

import cats.MonadError
import cats.data.{Kleisli, OptionT}
import cats.implicits._
import io.kafka4s.{BatchConsumer, Consumer, Producer}

import scala.util.control.NonFatal

class DeadLetterQueue[F[_]] private (producer: Producer[F], dlq: DeadLetter[F])(implicit F: MonadError[F, Throwable]) {

  def forConsumer(consumer: Consumer[F]): Consumer[F] = Kleisli { record =>
    OptionT(F.recoverWith(consumer.apply(record).value) {
      case NonFatal(ex) =>
        for {
          r <- dlq.build(record, ex)
          _ <- producer.send(r)
        } yield Some(())
    })
  }

  def forBatchConsumer(consumer: BatchConsumer[F]): BatchConsumer[F] = Kleisli { records =>
    OptionT(F.recoverWith(consumer.apply(records).value) {
      case NonFatal(ex) =>
        for {
          r <- records.traverse(dlq.build(_, ex))
          _ <- r.traverse(producer.send)
        } yield Some(())
    })
  }
}

object DeadLetterQueue {

  def apply[F[_]: MonadError[*[_], Throwable]](producer: Producer[F], topicSuffix: String = "-dlq")(
    consumer: Consumer[F]): Consumer[F] =
    new DeadLetterQueue(producer, DeadLetter[F](topicSuffix))
      .forConsumer(consumer)

  def apply[F[_]: MonadError[*[_], Throwable]](producer: Producer[F], dlq: DeadLetter[F])(
    consumer: Consumer[F]): Consumer[F] =
    new DeadLetterQueue[F](producer, dlq).forConsumer(consumer)
}
