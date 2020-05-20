package io.kafka4s.fs2

import cats.effect.{Blocker, Clock, ContextShift, IO, Resource, Timer}
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import io.kafka4s.Producer
import io.kafka4s.syntax._
import io.kafka4s.serdes.implicits._
import io.kafka4s.consumer.{BatchConsumer, Consumer, ConsumerRecord}
import io.kafka4s.effect.admin.KafkaAdminBuilder
import io.kafka4s.effect.consumer.{BatchKafkaConsumerBuilder, KafkaConsumerBuilder}
import io.kafka4s.effect.producer.KafkaProducerBuilder
import io.kafka4s.fs2.consumer.Fs2KafkaConsumerBuilder
import org.apache.kafka.clients.admin.NewTopic
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.concurrent.duration._

class Fs2KafkaSpec extends AnyFlatSpec with Matchers { self =>
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]               = IO.timer(ExecutionContext.global)
  val blocker                                 = Blocker.liftExecutionContext(ExecutionContext.global)

  val foo  = "fs2-foo"
  val boom = "fs2-boom"

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

  def withSingleRecord[A](topics: String*)(test: (Producer[IO], Deferred[IO, ConsumerRecord[IO]]) => IO[A]): A = {
    for {
      _           <- executionTime
      _           <- prepareTopics(topics)
      firstRecord <- Resource.liftF(Deferred[IO, ConsumerRecord[IO]])
      _ <- Fs2KafkaConsumerBuilder[IO](blocker)
        .withTopics(topics: _*)
        .withConsumer(Consumer.of[IO] {
          case Topic("fs2-boom") => IO.raiseError(new Exception("Somebody set up us the bomb"))
          case msg               => firstRecord.complete(msg)
        })
        .resource

      producer <- KafkaProducerBuilder[IO].resource

    } yield (producer, firstRecord)
  }.use(test.tupled).unsafeRunSync()

//  def withMultipleRecords[A](topics: String*)(test: (Producer[IO], Ref[IO, List[ConsumerRecord[IO]]]) => IO[A]): A = {
//    for {
//      _       <- executionTime
//      _       <- prepareTopics(topics)
//      records <- Resource.liftF(Ref[IO].of(List.empty[ConsumerRecord[IO]]))
//      _ <- KafkaConsumerBuilder[IO](blocker)
//        .withTopics(topics.toSet)
//        .withConsumer(Consumer.of[IO] {
//          case Topic("fs2-boom") => IO.raiseError(new Exception("Somebody set up us the bomb"))
//          case msg               => records.update(_ :+ msg)
//        })
//        .resource
//
//      producer <- KafkaProducerBuilder[IO].resource
//
//    } yield (producer, records)
//  }.use(test.tupled).unsafeRunSync()
//
//  def withRecordsBatch[A](topics: String*)(test: (Producer[IO], Ref[IO, List[ConsumerRecord[IO]]]) => IO[A]): A = {
//    for {
//      _       <- executionTime
//      _       <- prepareTopics(topics)
//      records <- Resource.liftF(Ref[IO].of(List.empty[ConsumerRecord[IO]]))
//      _ <- BatchKafkaConsumerBuilder[IO]
//        .withTopics(topics.toSet)
//        .withConsumer(BatchConsumer.of[IO] {
//          case BatchTopic("fs2-boom") => IO.raiseError(new Exception("Somebody set up us the bomb"))
//          case batch                  => records.update(_ ++ batch.toList)
//        })
//        .resource
//
//      producer <- KafkaProducerBuilder[IO].resource
//
//    } yield (producer, records)
//  }.use(test.tupled).unsafeRunSync()

  behavior of "Fs2KafkaConsumer"

  it should "should produce and consume messages" in withSingleRecord(topics = foo) { (producer, maybeMessage) =>
    for {
      _ <- producer.send(foo, key = 1, value = "bar")
      record <- waitFor(60.seconds) {
        maybeMessage.get
      }
      topic = record.topic
      key   <- record.key[Int]
      value <- record.as[String]
    } yield {
      topic shouldBe foo
      key shouldBe 1
      value shouldBe "bar"
    }
  }
}
