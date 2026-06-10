package io.kafka4s.consumer

import cats.Id
import io.kafka4s.common.Headers
import io.kafka4s.test.UnitSpec
import io.kafka4s.implicits._
import org.apache.kafka.clients.consumer.{ConsumerRecord => ApacheConsumerRecord}
import org.apache.kafka.common.record.TimestampType

import java.time.Instant
import scala.language.implicitConversions

class ConsumerRecordSpec extends UnitSpec {

  implicit def strToBytes(value: String): Array[Byte] = stringSerde.serialize(value).fold(throw _, identity)

  private val baseRecord = new ConsumerRecord[Id](
    topic = "lorem-ipsum",
    keyBytes = "lorem",
    valueBytes = "Lorem ipsum dolor amet",
    offset = 100,
    partition = 0,
    headers = Headers.empty[Id],
    timestamp = Instant.now()
  )

  "#apply" should "create a record from an org.apache.kafka.clients.consumer.ConsumerRecord" in {
    val record: DefaultConsumerRecord = new ApacheConsumerRecord(
      baseRecord.topic,
      baseRecord.partition,
      baseRecord.offset,
      baseRecord.timestamp.toEpochMilli,
      TimestampType.CREATE_TIME,
      0L,
      baseRecord.keyBytes.length,
      baseRecord.valueBytes.length,
      baseRecord.keyBytes,
      baseRecord.valueBytes
    )

    ConsumerRecord[Id](record) shouldBe baseRecord
  }

  "#of" should "create a instance from a topic and value pair tuple" is pending

  it should "create a instance from a topic and value" is pending

  it should "create a instance from a topic, key and value" is pending

  it should "create a instance from a topic, key, value and partition" is pending

  it should "create a instance from a topic, key, value, partition and offset" is pending

  "#show" should "render the topic, partition and offset"
}
