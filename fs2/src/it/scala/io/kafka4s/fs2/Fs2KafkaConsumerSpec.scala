package io.kafka4s.fs2

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{IO, Resource}
import cats.implicits._
import io.kafka4s.Producer
import io.kafka4s.consumer.{Consumer, ConsumerRecord}
import io.kafka4s.dsl._
import io.kafka4s.effect.producer.KafkaProducerBuilder
import io.kafka4s.fs2.consumer.Fs2KafkaConsumerBuilder
import io.kafka4s.serdes.implicits._

import scala.concurrent.duration._

class Fs2KafkaConsumerSpec extends IntegrationSpec {
  val foo  = "fs2-foo"
  val boom = "fs2-boom"

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
}
