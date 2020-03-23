package io.kafka4s.common

import cats.Id
import io.kafka4s.implicits._
import io.kafka4s.test.UnitSpec

class HeaderSpec extends UnitSpec {

  "$.apply" should "create a Header instance from a org.apache.kafka.common.header.Header instance" in {
    val recordHeader =
      new org.apache.kafka.common.header.internals.RecordHeader("foo", "bar".unsafeSerialize)
    val header = Header[Id](recordHeader)
    header.key shouldBe "foo"
    header.as[String] shouldBe "bar"
  }

  "$.of" should "create a Header instance from a key and value tuple" in {
    val header = Header.of[Id]("foo" -> "bar")
    header.key shouldBe "foo"
    header.as[String] shouldBe "bar"
  }

  it should "create an Header instance from a key and value" in {
    val header = Header.of[Id]("foo", "bar")
    header.key shouldBe "foo"
    header.as[String] shouldBe "bar"
  }

  "#as" should "convert the value into the given type" in {
    val header = Header[Id](key = "foo", value = 100.unsafeSerialize)
    header.key shouldBe "foo"
    header.as[Int] shouldBe 100
  }

  "ToKafka[_]" should "return a org.apache.kafka.common.header.Header instance" in {
    val header = Header.of[Id]("foo", "bar")
    val kafka  = ToKafka.convert(header).asInstanceOf[org.apache.kafka.common.header.Header]

    kafka shouldBe a[org.apache.kafka.common.header.Header]
    kafka.key() shouldBe header.key
    kafka.value() shouldBe header.value
  }
}
