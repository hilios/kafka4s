package io.kafka4s.effect

import cats.effect.concurrent.Ref
import cats.effect.{IO, Resource}
import cats.implicits._
import io.kafka4s.Producer
import io.kafka4s.consumer.ConsumerRecord
import io.kafka4s.consumer.batch._
import io.kafka4s.consumer.batch.dsl._
import io.kafka4s.effect.consumer.batch.BatchKafkaConsumerBuilder
import io.kafka4s.effect.producer.KafkaProducerBuilder
import io.kafka4s.implicits._

import scala.concurrent.duration._

class BatchKafkaSpec extends IntegrationSpec {

  val foo  = "foo"
  val boom = "boom"

  def withRecordsBatch[A](topics: String*)(test: (Producer[IO], Ref[IO, List[ConsumerRecord[IO]]]) => IO[A]): A = {
    for {
      _       <- executionTime
      _       <- prepareTopics(topics)
      records <- Resource.liftF(Ref[IO].of(List.empty[ConsumerRecord[IO]]))
      _ <- BatchKafkaConsumerBuilder[IO]
        .withTopics(topics.toSet)
        .withConsumer(BatchConsumer.of[IO] {
          case Topic("boom") => IO.raiseError(new Exception("Somebody set up us the bomb"))
          case batch         => records.update(_ ++ batch.toList)
        })
        .resource

      producer <- KafkaProducerBuilder[IO].resource

    } yield (producer, records)
  }.use(test.tupled).unsafeRunSync()

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
