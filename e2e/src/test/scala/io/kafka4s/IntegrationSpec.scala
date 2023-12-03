package io.kafka4s

import cats.effect.Blocker
import cats.effect.Clock
import cats.effect.ContextShift
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Timer
import cats.implicits._
import com.dimafeng.testcontainers.ContainerDef
import com.dimafeng.testcontainers.DockerComposeContainer
import com.dimafeng.testcontainers.ExposedService
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import io.kafka4s.effect.admin.KafkaAdminBuilder
import org.apache.kafka.clients.admin.NewTopic
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.TimeoutException
import scala.concurrent.duration._


trait IntegrationSpec extends AnyFlatSpec with Matchers with TestContainerForAll {
  private val blockingExecution = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]               = IO.timer(ExecutionContext.global)
  val blocker: Blocker                        = Blocker.liftExecutionContext(blockingExecution)

  override val containerDef: ContainerDef = DockerComposeContainer.Def(
    new File("docker-compose.yml"),
    tailChildContainers = true,
    exposedServices = Seq(
      ExposedService("kafka", 9092, Wait.forListeningPort())
    )
  )

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
        case Left(_)  => isReady.cancel *> IO.raiseError(new TimeoutException(duration.toString()))
        case Right(_) => IO.unit
      }
    } yield ()
  }

  def executionTime: Resource[IO, Long] =
    Resource.make(Clock[IO].monotonic(MILLISECONDS))(t0 =>
      for {
        t1 <- Clock[IO].monotonic(MILLISECONDS)
        t = FiniteDuration(t1 - t0, SECONDS)
        _ <- IO(println(s"Test completed in $t"))
      } yield ())

  def prepareTopics(topics: Seq[String]): Resource[IO, Unit] =
    for {
      admin <- KafkaAdminBuilder[IO].resource
      newTopics = topics.map(new NewTopic(_, 1, 1.toShort))
      _ <- Resource.make(admin.createTopics(newTopics))(_ => admin.deleteTopics(topics)).attempt
    } yield ()
}
