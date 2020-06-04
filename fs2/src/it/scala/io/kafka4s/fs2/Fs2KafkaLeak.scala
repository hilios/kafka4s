package io.kafka4s.fs2

import cats.effect.{Blocker, ExitCode, IO, IOApp, Resource}
import cats.implicits._
import io.kafka4s.consumer.Consumer
import io.kafka4s.effect.admin.KafkaAdminBuilder
import io.kafka4s.effect.log.slf4j.Slf4jLogger
import io.kafka4s.effect.producer.KafkaProducerBuilder
import io.kafka4s.fs2.consumer.{Fs2KafkaConsumer, Fs2KafkaConsumerBuilder}
import io.kafka4s.serdes.implicits._
import org.apache.kafka.clients.admin.NewTopic

import scala.concurrent.duration._

object Fs2KafkaLeak extends IOApp {

  val topics = Seq("foo")

  def resource =
    for {
      admin <- KafkaAdminBuilder[IO].resource
      newTopics = topics.map(new NewTopic(_, 1, 1))
      _ <- Resource.make(admin.createTopics(newTopics))(_ => admin.deleteTopics(topics)).attempt
      // Start resources
      logger   <- Resource.liftF(Slf4jLogger[IO].of[Fs2KafkaConsumer[Any]])
      blocker  <- Blocker[IO]
      producer <- KafkaProducerBuilder[IO].resource
    } yield (blocker, producer, logger)

  def run(args: List[String]): IO[ExitCode] = resource.use {
    case (blocker, producer, logger) =>
      def increment(topic: String, count: Long = 1L): IO[Unit] = {
        val msg = producer.send(topic -> s"Message #$count") >> timer.sleep(2.seconds)
        msg.flatMap(_ => increment(topic, count + 1))
      }

      val consumerApp = Consumer.of[IO] {
        case msg =>
          for {
            b <- msg.as[String]
            _ <- logger.info(s"Incoming message ${msg.topic}: $b")
          } yield ()
      }

      for {
        _ <- increment(topics.head).start
        e <- Fs2KafkaConsumerBuilder[IO](blocker)
          .withTopics(topics.toSet)
          .withConsumer(consumerApp)
          .serve
          .as(ExitCode.Success)
      } yield e
  }
}
