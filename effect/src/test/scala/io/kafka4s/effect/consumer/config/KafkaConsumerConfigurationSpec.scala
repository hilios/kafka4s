package io.kafka4s.effect.consumer.config

import java.util.Properties

import cats.effect.SyncIO
import io.kafka4s.effect.test.UnitSpec
import org.apache.kafka.clients.consumer.ConsumerConfig

class KafkaConsumerConfigurationSpec extends UnitSpec {
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test")

  "$.loadFrom" should "extract the bootstrap servers and group id from the properties" in {
    val config = SyncIO.fromEither(KafkaConsumerConfiguration.loadFrom(props)).unsafeRunSync()
    config.bootstrapServers shouldBe Seq("localhost:9092")
    config.groupId shouldBe "test"
  }

  "$.load" should "load the configuration from the HOCON file" in {
    val config = SyncIO.fromEither(KafkaConsumerConfiguration.load).unsafeRunSync()
    config.bootstrapServers shouldBe Seq("config:9092")
    config.groupId shouldBe "config-test"
  }
}
