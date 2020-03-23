package io.kafka4s.common

trait ToKafka[A] {
  type Result

  def convert(a: A): Result
}

object ToKafka {
  def apply[A](implicit T: ToKafka[A]): ToKafka[A] = T

  def convert[A](a: A)(implicit T: ToKafka[A]): T.Result = T.convert(a)
}
