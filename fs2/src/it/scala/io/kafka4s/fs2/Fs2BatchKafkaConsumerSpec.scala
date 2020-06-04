package io.kafka4s.fs2

import cats.effect.concurrent.Ref
import cats.effect.{IO, Resource}
import cats.implicits._
import io.kafka4s.Producer
import io.kafka4s.consumer.ConsumerRecord
import io.kafka4s.consumer.batch.BatchConsumer
import io.kafka4s.consumer.batch.dsl._
import io.kafka4s.effect.producer.KafkaProducerBuilder
import io.kafka4s.fs2.consumer.batch.Fs2BatchKafkaConsumerBuilder
import io.kafka4s.serdes.implicits._

import scala.concurrent.duration._

class Fs2BatchKafkaConsumerSpec extends IntegrationSpec {

  val foo  = "fs2-batch-foo"
  val boom = "fs2-batch-boom"

  def withBatchConsumer[A](topics: String*)(test: (Producer[IO], Ref[IO, List[ConsumerRecord[IO]]]) => IO[A]): A = {
    for {
      _       <- executionTime
      _       <- prepareTopics(topics)
      records <- Resource.liftF(Ref[IO].of(List.empty[ConsumerRecord[IO]]))
      consumer = Fs2BatchKafkaConsumerBuilder[IO](blocker)
        .withTopics(topics: _*)
        .withConsumer(BatchConsumer.of[IO] {
          case Topic("fs2-boom") => IO.raiseError(new Exception("Somebody set up us the bomb"))
          case batch             => records.update(_ ++ batch.toList)
        })
      _        <- Resource.make(consumer.serve.start)(c => c.cancel)
      producer <- KafkaProducerBuilder[IO].resource
    } yield (producer, records)
  }.use(test.tupled).unsafeRunSync()

  behavior of "Fs2BatchKafkaConsumer"

  it should "should produce and consume batch of messages" in
    withBatchConsumer(topics = foo) { (producer, records) =>
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
    withBatchConsumer(topics = foo, boom) { (producer, records) =>
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
