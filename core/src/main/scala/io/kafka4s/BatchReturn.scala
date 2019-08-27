package io.kafka4s

import cats.data.NonEmptyList

sealed trait BatchReturn[F[_]] {
  def records: NonEmptyList[Record[F]]
}

object BatchReturn {
  final case class Ack[F[_]](records: NonEmptyList[Record[F]]) extends BatchReturn[F]
  final case class Err[F[_]](records: NonEmptyList[Record[F]], ex: Exception) extends BatchReturn[F]
}
