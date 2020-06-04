package io.kafka4s.consumer.batch

import cats.ApplicativeError

trait BatchConsumerImplicitsOps {
  implicit final class BatchConsumerOps[F[_]](val consumer: BatchConsumer[F]) {

    def orNotFound(implicit F: ApplicativeError[F, Throwable]): BatchRecordConsumer[F] =
      BatchConsumer.orNotFound(consumer)
  }
}
