package io.kafka4s.middlewares.dlq

import cats.Monad
import cats.MonadError
import cats.Semigroup
import cats.implicits._
import io.kafka4s.common.Header
import io.kafka4s.common.Record
import io.kafka4s.producer.ProducerRecord
import io.kafka4s.serdes.implicits._

import java.io.PrintWriter
import java.io.StringWriter

trait DeadLetter[F[_]] {
  def build(record: Record[F], throwable: Throwable): F[Record[F]]
}

object DeadLetter {

  def prefixed(prefix: String): String => String = root => s"$root$prefix"

  def apply[F[_]: MonadError[*[_], Throwable]](prefix: String): DeadLetter[F] =
    DeadLetter[F](prefixed(prefix))

  def apply[F[_]: MonadError[*[_], Throwable]](fn: String => String): DeadLetter[F] =
    (record, ex) =>
      (
        Header.of[F]("DLQ-Origin"            -> record.topic),
        Header.of[F]("DLQ-Exception-Message" -> getMessage(ex)),
        Header.of[F]("DLQ-Stack-Trace"       -> getStackTrace(ex)),
      ).mapN {
        case (topic, message, stackTrace) =>
          ProducerRecord[F](record).put(topic, message, stackTrace).copy(topic = fn(record.topic))
    }

  /** Gets a short message summarising the exception in the form
    * {ClassNameWithoutPackage}: {ThrowableMessage}
    *
    * Extracted from org.apache.commons.lang3.exception.ExceptionUtils.getMessage
    */
  def getMessage(throwable: Throwable): String = Option(throwable).fold("") { e =>
    s"${e.getClass.getSimpleName}: ${e.getMessage}"
  }

  /** Throw a checked exception without adding the exception to the throws
    * clause of the calling method.
    *
    * Extracted from org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace
    */
  def getStackTrace(throwable: Throwable): String = {
    val sw = new StringWriter
    val pw = new PrintWriter(sw, true)
    throwable.printStackTrace(pw)
    sw.getBuffer.toString
  }

  implicit def semigroup[F[_]: Monad]: Semigroup[DeadLetter[F]] = new Semigroup[DeadLetter[F]] {
    override def combine(x: DeadLetter[F], y: DeadLetter[F]): DeadLetter[F] =
      (record, ex) => x.build(record, ex).flatMap(y.build(_, ex))
  }
}
