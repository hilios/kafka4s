package io.kafka4s.effect.log.slf4j

import cats.effect.Sync
import cats.implicits._
import io.kafka4s.effect.log.{Level, Logger, Message}
import izumi.reflect.{Tag, TagK, TagT}
import org.slf4j

class Slf4jLogger[F[_]] private (logger: slf4j.Logger)(implicit F: Sync[F]) extends Logger[F] {

  def log(message: Message): F[Unit] = F.delay {
    message match {
      case Level.Trace(msg, ex) => ex.fold(logger.trace(msg))(logger.trace(msg, _))
      case Level.Debug(msg, ex) => ex.fold(logger.debug(msg))(logger.debug(msg, _))
      case Level.Info(msg, ex)  => ex.fold(logger.info(msg))(logger.info(msg, _))
      case Level.Warn(msg, ex)  => ex.fold(logger.warn(msg))(logger.warn(msg, _))
      case Level.Error(msg, ex) => ex.fold(logger.error(msg))(logger.error(msg, _))
    }
  }
}

object Slf4jLogger {

  private[kafka4s] final class Slf4jLoggerPartiallyApplied[F[_]](val dummy: Boolean = false) extends AnyVal {

    def of(name: String)(implicit F: Sync[F]): F[Slf4jLogger[F]] =
      for {
        logger <- F.delay(slf4j.LoggerFactory.getLogger(name))
      } yield new Slf4jLogger(logger)

    def of[A](implicit F: Sync[F], tag: Tag[A]): F[Slf4jLogger[F]] = of(tag.closestClass.getCanonicalName)

    def ofK[A[_]](implicit F: Sync[F], tag: TagK[A]): F[Slf4jLogger[F]] = of(tag.closestClass.getCanonicalName)

    def ofT[A[_[_]]](implicit F: Sync[F], tag: TagT[A]): F[Slf4jLogger[F]] = of(tag.closestClass.getCanonicalName)
  }

  def apply[F[_]] = new Slf4jLoggerPartiallyApplied[F]()
}
