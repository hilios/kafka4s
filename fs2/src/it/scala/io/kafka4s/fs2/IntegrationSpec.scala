package io.kafka4s.fs2

import cats.effect.{Blocker, Clock, ContextShift, IO, Resource, Timer}
import cats.implicits._
import io.kafka4s.effect.admin.KafkaAdminBuilder
import org.apache.kafka.clients.admin.NewTopic
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, TimeoutException}

trait IntegrationSpec extends AnyFlatSpec with Matchers { self: AnyFlatSpec =>
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]               = IO.timer(ExecutionContext.global)
  val blocker                                 = Blocker.liftExecutionContext(ExecutionContext.global)

  def waitFor[A](duration: FiniteDuration)(ioa: => IO[A]): IO[A] =
    IO.race(Timer[IO].sleep(duration), ioa).flatMap {
      case Left(_)  => IO.raiseError(new TimeoutException(duration.toString()))
      case Right(a) => IO.pure(a)
    }

  def waitUntil(duration: FiniteDuration, tryEvery: FiniteDuration = 10.millis)(predicate: IO[Boolean]): IO[Unit] = {
    def loop: IO[Unit] =
      for {
        _  <- Timer[IO].sleep(tryEvery)
        ok <- predicate
        _  <- if (ok) IO.unit else loop
      } yield ()

    for {
      isReady <- loop.start
      _ <- IO.race(Timer[IO].sleep(duration), isReady.join).flatMap {
        case Left(_)  => isReady.cancel >> IO.raiseError(new TimeoutException(duration.toString()))
        case Right(_) => IO.unit
      }
    } yield ()
  }

  def executionTime: Resource[IO, Long] =
    Resource.make(Clock[IO].monotonic(MILLISECONDS))(t0 =>
      for {
        t1 <- Clock[IO].monotonic(MILLISECONDS)
        t = FiniteDuration(t1 - t0, SECONDS)
        _ <- IO(info(s"Test completed in $t"))
      } yield ())

  def prepareTopics(topics: Seq[String]): Resource[IO, Unit] =
    for {
      admin <- KafkaAdminBuilder[IO].resource
      newTopics = topics.map(new NewTopic(_, 1, 1))
      _ <- Resource.make(admin.createTopics(newTopics))(_ => admin.deleteTopics(topics)).attempt
    } yield ()
}
