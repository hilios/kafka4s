package io.kafka4s.effect.consumer

import java.time.{Duration => JDuration}
import java.util.Properties

import cats.Applicative
import cats.data.Kleisli
import cats.effect._
import cats.implicits._
import io.kafka4s.consumer._
import io.kafka4s.effect.log.Logger
import io.kafka4s.effect.properties.implicits._
import io.kafka4s.effect.log.slf4j.Slf4jLogger
import io.kafka4s.effect.utils.ThreadSafeBlocker
import org.apache.kafka.clients.consumer.{
  ConsumerConfig,
  ConsumerRebalanceListener,
  OffsetAndMetadata,
  OffsetAndTimestamp,
  KafkaConsumer => ApacheKafkaConsumer
}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.matching.Regex

class ConsumerEffect[F[_]] private (consumer: DefaultConsumer,
                                    threadSafe: ThreadSafeBlocker[F],
                                    cb: ConsumerCallback[F])(implicit F: Effect[F], CS: ContextShift[F]) {

  val consumerRebalanceListener = new ConsumerRebalanceListener() {

    def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = {
      if (partitions.size() > 0)
        F.toIO(cb(ConsumerRebalance.PartitionsRevoked(partitions.asScala.toSeq))).unsafeRunSync()
    }

    def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = {
      if (partitions.size() > 0)
        F.toIO(cb(ConsumerRebalance.PartitionsAssigned(partitions.asScala.toSeq))).unsafeRunSync()
    }
  }

  def assign(partitions: Seq[TopicPartition]): F[Unit] = threadSafe.delay(consumer.assign(partitions.asJava))

  def subscribe(topics: Seq[String]): F[Unit] =
    threadSafe.delay(consumer.subscribe(topics.asJava, consumerRebalanceListener))

  def subscribe(regex: Regex): F[Unit] =
    threadSafe.delay(consumer.subscribe(regex.pattern, consumerRebalanceListener))

  def unsubscribe: F[Unit] = threadSafe.delay(consumer.unsubscribe())

  def pause(partitions: Seq[TopicPartition]): F[Unit]  = threadSafe.delay(consumer.pause(partitions.asJava))
  def resume(partitions: Seq[TopicPartition]): F[Unit] = threadSafe.delay(consumer.resume(partitions.asJava))
  def paused: F[Set[TopicPartition]]                   = threadSafe.delay(consumer.paused().asScala.toSet)
  def wakeup: F[Unit]                                  = F.delay(consumer.wakeup())

  def subscription: F[Set[String]]       = threadSafe.delay(consumer.subscription().asScala.toSet)
  def assignment: F[Set[TopicPartition]] = threadSafe.delay(consumer.assignment().asScala.toSet)

  def listTopics: F[Map[String, Seq[PartitionInfo]]] =
    threadSafe.delay(consumer.listTopics().asScala.toMap.mapValues(_.asScala.toSeq))

  def position(partition: TopicPartition): F[Long]               = threadSafe.delay(consumer.position(partition))
  def committed(partition: TopicPartition): F[OffsetAndMetadata] = threadSafe.delay(consumer.committed(partition))

  def beginningOffsets(partitions: Seq[TopicPartition]): F[Map[TopicPartition, Long]] =
    threadSafe.delay(Map(consumer.beginningOffsets(partitions.asJavaCollection).asScala.mapValues(Long2long).toSeq: _*))

  def endOffsets(partitions: Seq[TopicPartition]): F[Map[TopicPartition, Long]] =
    threadSafe.delay(Map(consumer.endOffsets(partitions.asJavaCollection).asScala.mapValues(Long2long).toSeq: _*))

  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]): F[Map[TopicPartition, OffsetAndTimestamp]] =
    threadSafe.delay(
      Map(
        consumer
          .offsetsForTimes(timestampsToSearch.mapValues(long2Long).asJava)
          .asScala
          .toSeq: _*))

  def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
    threadSafe.delay(consumer.partitionsFor(topic).asScala.toVector)

  def seek(partition: TopicPartition, offset: Long): F[Unit] = blocker.delay(consumer.seek(partition, offset))

  def seekToBeginning(partitions: Seq[TopicPartition]): F[Unit] =
    threadSafe.delay(consumer.seekToBeginning(partitions.asJavaCollection))

  def seekToEnd(partitions: Seq[TopicPartition]): F[Unit] =
    threadSafe.delay(consumer.seekToEnd(partitions.asJavaCollection))

  def close(timeout: FiniteDuration = 30.seconds): F[Unit] =
    threadSafe.delay(consumer.close(JDuration.ofMillis(timeout.toMillis)))

  def commit(): F[Unit] =
    threadSafe.delay(consumer.commitSync())

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    threadSafe.delay(consumer.commitSync(offsets.asJava))

  def poll(timeout: FiniteDuration): F[Iterator[DefaultConsumerRecord]] =
    threadSafe.delay(consumer.poll(JDuration.ofMillis(timeout.toMillis)).iterator().asScala)
}

object ConsumerEffect {

  def noopCallback[F[_]: Applicative]: ConsumerCallback[F] = Kleisli(_ => Applicative[F].unit)

  def logPartitionsCallback[F[_]: Sync](logger: Logger[F], groupId: String): ConsumerCallback[F] = Kleisli {
    case ConsumerRebalance.PartitionsAssigned(p) =>
      logger.info(s"Partitions [${p.mkString(", ")}] assigned to the consumer group id [$groupId]")
    case ConsumerRebalance.PartitionsRevoked(p) =>
      logger.info(s"Partitions [${p.mkString(", ")}] revoked from the consumer consumer [$groupId]")
  }

  def apply[F[_]](properties: Properties, blocker: Blocker)(implicit F: ConcurrentEffect[F],
                                                            CS: ContextShift[F]): F[ConsumerEffect[F]] =
    for {
      consumer   <- F.delay(new ApacheKafkaConsumer[Array[Byte], Array[Byte]](properties))
      threadSafe <- ThreadSafeBlocker[F](blocker)
      logger     <- Slf4jLogger[F].ofT[ConcurrentEffect]
      groupId = properties.getter[String](ConsumerConfig.GROUP_ID_CONFIG).fold(_ => "undefined", identity)
    } yield new ConsumerEffect(consumer, threadSafe, logPartitionsCallback(logger, groupId))

  def resource[F[_]](properties: Properties, blocker: Blocker)(implicit F: ConcurrentEffect[F], CS: ContextShift[F]) =
    Resource.make(ConsumerEffect[F](properties, blocker))(c => c.wakeup >> c.close())
}
