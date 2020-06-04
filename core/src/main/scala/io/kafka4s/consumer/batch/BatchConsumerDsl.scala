package io.kafka4s.consumer.batch

import cats.data.NonEmptyList
import io.kafka4s.common.Record

private[kafka4s] trait BatchConsumerDsl {

  /**
    * Pattern matching for a batch of Records topic
    */
  object Topic {
    def unapply[F[_]](records: NonEmptyList[Record[F]]): Option[String] = Some(records.head.topic)
  }
}
