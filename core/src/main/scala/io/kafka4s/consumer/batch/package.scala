package io.kafka4s.consumer

import cats.data.{Kleisli, NonEmptyList, OptionT}

package object batch extends BatchConsumerImplicitsOps {
  type BatchConsumer[F[_]]       = Kleisli[OptionT[F, *], NonEmptyList[ConsumerRecord[F]], Unit]
  type BatchRecordConsumer[F[_]] = Kleisli[F, NonEmptyList[ConsumerRecord[F]], batch.BatchReturn[F]]

  object dsl extends BatchConsumerDsl
}
