package io.kafka4s.producer

import cats.Id
import io.kafka4s.common.{Header, ToKafka}
import io.kafka4s.test.UnitSpec

class ProducerRecordSpec extends UnitSpec {
  "$of" should "create an ProducerRecord instance from a topic and message tuple" in {
    val record = ProducerRecord.of[Id]("my-topic" -> "message")
    record.topic shouldBe "my-topic"
    record.as[String] shouldBe "message"
    record.key[Option[String]] shouldBe None
    record.partition shouldBe None
  }

  it should "create an ProducerRecord instance from a topic and message arguments" in {
    val record = ProducerRecord.of[Id]("my-topic", "message")
    record.topic shouldBe "my-topic"
    record.as[String] shouldBe "message"
    record.key[Option[String]] shouldBe None
    record.partition shouldBe None
  }

  it should "create an ProducerRecord instance from a topic, key and message" in {
    val record = ProducerRecord.of[Id]("my-topic", "foo", "bar")
    record.topic shouldBe "my-topic"
    record.key[String] shouldBe "foo"
    record.as[String] shouldBe "bar"
    record.partition shouldBe None
  }

  it should "create an ProducerRecord instance from a topic, key, message and partition" in {
    val record = ProducerRecord.of[Id]("my-topic", "foo", "bar", 0)
    record.topic shouldBe "my-topic"
    record.key[String] shouldBe "foo"
    record.as[String] shouldBe "bar"
    record.partition shouldBe Some(0)
  }

  "#put" should "add headers to the record" in {
    val record = ProducerRecord.of[Id]("my-topic", "foo", "bar")
    val header = Header.of[Id]("foo", "bar")
    record.put(header).headers.get("foo") shouldBe Some(header)
  }

  "ToKafka[_]" should "return a org.apache.kafka.clients.producer.ProducerRecord instance" in {
    val record         = ProducerRecord.of[Id]("my-topic", "foo", "bar")
    val producerRecord = ToKafka.convert(record).asInstanceOf[DefaultProducerRecord]

    producerRecord.topic() shouldBe record.topic
    producerRecord.key() shouldBe record.keyBytes
    producerRecord.value() shouldBe record.valueBytes
    producerRecord.partition() shouldBe null
  }
}
