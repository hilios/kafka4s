package io

import io.kafka4s.consumer.{ConsumerImplicits, ConsumerSyntax}
import io.kafka4s.serdes.SerdeImplicits

package object kafka4s {
  type Producer[F[_]]      = producer.Producer[F]
  type Consumer[F[_]]      = consumer.Consumer[F]
  type BatchConsumer[F[_]] = consumer.BatchConsumer[F]

  type RecordConsumer[F[_]]      = consumer.RecordConsumer[F]
  type BatchRecordConsumer[F[_]] = consumer.BatchRecordConsumer[F]

  object all extends ConsumerSyntax with ConsumerImplicits with SerdeImplicits
  object syntax extends ConsumerSyntax
  object implicits extends ConsumerImplicits with SerdeImplicits
}
