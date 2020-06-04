package io

package object kafka4s {
  type Producer[F[_]]            = producer.Producer[F]
  type Consumer[F[_]]            = consumer.Consumer[F]
  type RecordConsumer[F[_]]      = consumer.RecordConsumer[F]
  type BatchConsumer[F[_]]       = consumer.batch.BatchConsumer[F]
  type BatchRecordConsumer[F[_]] = consumer.batch.BatchRecordConsumer[F]

  object dsl extends consumer.ConsumerDsl

  object implicits
      extends serdes.SerdeImplicits
      with consumer.ConsumerImplicitOps
      with consumer.batch.BatchConsumerImplicitsOps
}
