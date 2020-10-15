package io.kafka4s.effect.utils

import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, Concurrent, ContextShift, Sync}
import cats.implicits._

import scala.concurrent.blocking

class ThreadSafeBlocker[F[_]] private (blocker: Blocker, semaphore: Semaphore[F])(implicit F: Sync[F],
                                                                                  CS: ContextShift[F]) {

  def delay[A](thunk: => A): F[A] = semaphore.withPermit(blocker.delay(blocking(thunk)))
}

object ThreadSafeBlocker {

  def apply[F[_]](blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F]): F[ThreadSafeBlocker[F]] =
    for {
      semaphore <- Semaphore[F](1)
    } yield new ThreadSafeBlocker(blocker, semaphore)
}
