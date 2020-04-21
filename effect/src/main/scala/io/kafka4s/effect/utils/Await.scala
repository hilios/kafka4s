package io.kafka4s.effect.utils

import java.util.concurrent.TimeUnit

import cats.effect.{Concurrent, Timer}
import cats.implicits._

import scala.concurrent.TimeoutException
import scala.concurrent.duration._

trait Await[F[_], G[_]] {
  def await[A](timeout: FiniteDuration)(ga: G[A]): F[A]
}

object Await {

  implicit def javaFutureEventually[F[_]](implicit F: Concurrent[F],
                                          T: Timer[F]): Await[F, java.util.concurrent.Future] =
    new Await[F, java.util.concurrent.Future] {

      /**
        * Loop until the future either done or cancelled
        */
      private def awaitLoop[T](future: java.util.concurrent.Future[T]): F[T] =
        if (future.isDone || future.isCancelled) F.delay(future.get(0, TimeUnit.NANOSECONDS))
        else awaitLoop(future)

      /**
        * Await the completion of the Future
        *
        * @param timeout the maximum duration to wait for a completion
        */
      def await[A](timeout: FiniteDuration)(future: java.util.concurrent.Future[A]): F[A] =
        Concurrent.timeoutTo(awaitLoop(future),
                             timeout,
                             F.delay(future.cancel(true)) *> F.raiseError(
                               new TimeoutException(s"Await did not complete after ${timeout.toMillis} ms")))
    }
}
