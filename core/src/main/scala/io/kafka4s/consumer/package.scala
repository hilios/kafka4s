package io.kafka4s

import cats.data.{Kleisli, OptionT}
import org.apache.kafka.clients.consumer.{Consumer => ApacheConsumer, ConsumerRecord => ApacheConsumerRecord}

package object consumer extends ConsumerImplicitOps {
  private[kafka4s] type DefaultConsumer       = ApacheConsumer[Array[Byte], Array[Byte]]
  private[kafka4s] type DefaultConsumerRecord = ApacheConsumerRecord[Array[Byte], Array[Byte]]

  type Consumer[F[_]]       = Kleisli[OptionT[F, *], ConsumerRecord[F], Unit]
  type RecordConsumer[F[_]] = Kleisli[F, ConsumerRecord[F], Return[F]]

  type ConsumerCallback[F[_]] = Kleisli[F, ConsumerRebalance, Unit]
}
