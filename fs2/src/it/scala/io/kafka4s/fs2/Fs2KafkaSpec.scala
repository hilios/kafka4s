package io.kafka4s.fs2

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Blocker, Clock, ContextShift, IO, Resource, Timer}
import cats.implicits._
import io.kafka4s.Producer
import io.kafka4s.consumer.{BatchConsumer, Consumer, ConsumerRecord}
import io.kafka4s.effect.admin.KafkaAdminBuilder
import io.kafka4s.effect.producer.KafkaProducerBuilder
import io.kafka4s.fs2.consumer.{Fs2BatchKafkaConsumerBuilder, Fs2KafkaConsumerBuilder}
import io.kafka4s.serdes.implicits._
import io.kafka4s.syntax._
import org.apache.kafka.clients.admin.NewTopic
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, TimeoutException}

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
      consumer = Fs2KafkaConsumerBuilder[IO](blocker)
        .withTopics(topics: _*)
        .withConsumer(Consumer.of[IO] {
          case Topic("fs2-boom") => IO.raiseError(new Exception("Somebody set up us the bomb"))
          case msg               => firstRecord.complete(msg)
        })
      _        <- Resource.make(consumer.serve.start)(c => c.cancel)
      producer <- KafkaProducerBuilder[IO].resource
    } yield (producer, firstRecord)
  }.use(test.tupled).unsafeRunSync()

  def withMultipleRecords[A](topics: String*)(test: (Producer[IO], Ref[IO, List[ConsumerRecord[IO]]]) => IO[A]): A = {
    for {
      _       <- executionTime
      _       <- prepareTopics(topics)
      records <- Resource.liftF(Ref[IO].of(List.empty[ConsumerRecord[IO]]))
      consumer = Fs2KafkaConsumerBuilder[IO](blocker)
        .withTopics(topics: _*)
        .withConsumer(Consumer.of[IO] {
          case Topic("fs2-boom") => IO.raiseError(new Exception("Somebody set up us the bomb"))
          case msg               => records.update(_ :+ msg)
        })
      _        <- Resource.make(consumer.serve.start)(c => c.cancel)
      producer <- KafkaProducerBuilder[IO].resource
    } yield (producer, records)
  }.use(test.tupled).unsafeRunSync()

  def withRecordsBatch[A](topics: String*)(test: (Producer[IO], Ref[IO, List[ConsumerRecord[IO]]]) => IO[A]): A = {
    for {
      _       <- executionTime
      _       <- prepareTopics(topics)
      records <- Resource.liftF(Ref[IO].of(List.empty[ConsumerRecord[IO]]))
      consumer = Fs2BatchKafkaConsumerBuilder[IO](blocker)
        .withTopics(topics: _*)
        .withConsumer(BatchConsumer.of[IO] {
          case BatchTopic("fs2-boom") => IO.raiseError(new Exception("Somebody set up us the bomb"))
          case batch                  => records.update(_ ++ batch.toList)
        })
      _        <- Resource.make(consumer.serve.start)(c => c.cancel)
      producer <- KafkaProducerBuilder[IO].resource
    } yield (producer, records)
  }.use(test.tupled).unsafeRunSync()

  behavior of "Fs2KafkaConsumer"

  it should "should produce and consume messages" in withSingleRecord(topics = foo) { (producer, maybeMessage) =>
    for {
      _ <- producer.send(foo, key = 1, value = "bar")
      record <- waitFor(30.seconds) {
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

  it should "should produce and consume multiple messages" in withMultipleRecords(topics = foo) { (producer, records) =>
    for {
      _ <- (1 to 100).toList.traverse(n => producer.send(foo, value = s"bar #$n"))
      _ <- waitUntil(30.seconds) {
        records.get.map(_.length == 100)
      }
      len    <- records.get.map(_.length)
      record <- records.get.flatMap(l => IO(l.last))
      topic = record.topic
      key   <- record.key[Option[Int]]
      value <- record.as[String]
    } yield {
      len shouldBe 100
      topic shouldBe foo
      key shouldBe None
      value shouldBe "bar #100"
    }
  }

  it should "should not stop consuming even if there is an exception in the consumer" in
    withMultipleRecords(topics = foo, boom) { (producer, records) =>
      for {
        _ <- (1 to 50).toList.traverse(n => producer.send(foo, value = s"bar #$n"))
        _ <- producer.send(boom, value = "All your base are belong to us.")
        _ <- (51 to 100).toList.traverse(n => producer.send(foo, value = s"bar #$n"))
        _ <- waitUntil(30.seconds) {
          records.get.map(_.length == 100)
        }
        len    <- records.get.map(_.length)
        record <- records.get.flatMap(l => IO(l.last))
        topic = record.topic
        key   <- record.key[Option[Int]]
        value <- record.as[String]
      } yield {
        len shouldBe 100
        topic shouldBe foo
        key shouldBe None
        value shouldBe "bar #100"
      }
    }

  behavior of "Fs2BatchKafkaConsumer"

  it should "should produce and consume batch of messages" in
    withRecordsBatch(topics = foo) { (producer, records) =>
      for {
        _ <- (1 to 100).toList.traverse(n => producer.send(foo, value = s"bar #$n"))
        _ <- waitUntil(30.seconds) {
          records.get.map(_.length == 100)
        }
        len    <- records.get.map(_.length)
        record <- records.get.flatMap(l => IO(l.last))
        topic = record.topic
        key   <- record.key[Option[Int]]
        value <- record.as[String]
      } yield {
        len shouldBe 100
        topic shouldBe foo
        key shouldBe None
        value shouldBe "bar #100"
      }
    }

  it should "should not stop consuming even if there is an exception in the consumer" in
    withRecordsBatch(topics = foo, boom) { (producer, records) =>
      for {
        _ <- (1 to 50).toList.traverse(n => producer.send(foo, value = s"bar #$n"))
        _ <- producer.send(boom, value = "All your base are belong to us.")
        _ <- (51 to 100).toList.traverse(n => producer.send(foo, value = s"bar #$n"))
        _ <- waitUntil(30.seconds) {
          records.get.map(_.length == 100)
        }
        len    <- records.get.map(_.length)
        record <- records.get.flatMap(l => IO(l.last))
        topic = record.topic
        key   <- record.key[Option[Int]]
        value <- record.as[String]
      } yield {
        len shouldBe 100
        topic shouldBe foo
        key shouldBe None
        value shouldBe "bar #100"
      }
    }
}
