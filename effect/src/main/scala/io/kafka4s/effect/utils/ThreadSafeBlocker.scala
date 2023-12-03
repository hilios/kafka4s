package io.kafka4s.effect.utils

import cats.effect.Blocker
import cats.effect.Concurrent
import cats.effect.ContextShift
import cats.effect.Resource
import cats.effect.Sync
import cats.effect.concurrent.Semaphore
import cats.implicits._

class ThreadSafeBlocker[F[_]] private (blocker: Blocker, semaphore: Semaphore[F])(implicit F: Sync[F],
                                                                                  CS: ContextShift[F]) {
  def delay[A](thunk: => A): F[A] =
    Resource.make(semaphore.acquire)(_ => semaphore.release).use(_ => blocker.delay(thunk))
}

object ThreadSafeBlocker {

  def apply[F[_]](blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F]): F[ThreadSafeBlocker[F]] =
    for {
      semaphore <- Semaphore[F](1)
    } yield new ThreadSafeBlocker(blocker, semaphore)
}
