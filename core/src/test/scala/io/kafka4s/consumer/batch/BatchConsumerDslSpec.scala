package io.kafka4s.consumer.batch

import cats.Id
import cats.data.NonEmptyList
import io.kafka4s.consumer.ConsumerRecord
import io.kafka4s.consumer.batch.dsl._
import io.kafka4s.implicits._
import io.kafka4s.test.UnitSpec

class BatchConsumerDslSpec extends UnitSpec {

  val record: ConsumerRecord[Id] =
    ConsumerRecord
      .of[Id](topic = "foo", key = "bar", value = "Hello, World!", partition = 0, offset = 1234L)

  "Topic(name)" should "extract the topic name from a non empty list of consumer records" in {
    val Topic(name) = NonEmptyList.one(record)
    name shouldBe record.topic
  }
}
