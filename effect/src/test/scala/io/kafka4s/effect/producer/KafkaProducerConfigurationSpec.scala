package io.kafka4s.effect.producer

import java.util.Properties

import cats.effect.SyncIO
import io.kafka4s.effect.test.UnitSpec
import io.kafka4s.effect.producer.adts._
import org.apache.kafka.clients.producer.ProducerConfig

class KafkaProducerConfigurationSpec extends UnitSpec {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  "$loadFrom" should "extract the bootstrap servers from the properties" in {
    val config = SyncIO.fromEither(KafkaProducerConfiguration.loadFrom(props)).unsafeRunSync()
    config.bootstrapServers shouldBe Seq("localhost:9092")
    config.compression shouldBe CompressionType.None
    config.enableIdempotent shouldBe false
    config.acks shouldBe Acks.One
  }

  "$load" should "load the configuration from the HOCON file" in {
    val config = SyncIO.fromEither(KafkaProducerConfiguration.load).unsafeRunSync()
    config.bootstrapServers shouldBe Seq("config:9092")
    config.compression shouldBe CompressionType.Gzip
    config.enableIdempotent shouldBe true
    config.acks shouldBe Acks.All
  }
}
