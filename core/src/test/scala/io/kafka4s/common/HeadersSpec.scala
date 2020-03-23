package io.kafka4s.common

import cats.Id
import io.kafka4s.test.UnitSpec

class HeadersSpec extends UnitSpec {
  it should "do something" is pending

  "ToKafka[_]" should "return a org.apache.kafka.common.header.Headers instance" in {
    val header  = Header.of[Id]("foo", "bar")
    val headers = Headers(header)
    val kafka   = ToKafka.convert(headers).asInstanceOf[org.apache.kafka.common.header.Headers]

    kafka shouldBe a[org.apache.kafka.common.header.Headers]
    kafka.lastHeader("foo").key() shouldBe "foo"
  }
}
