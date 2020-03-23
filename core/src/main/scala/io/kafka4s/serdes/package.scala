package io.kafka4s

package object serdes {
  type Result[T] = Either[Throwable, T]

  object implicits extends SerdeImplicits
}
