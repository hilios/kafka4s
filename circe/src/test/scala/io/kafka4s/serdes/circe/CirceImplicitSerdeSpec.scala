package io.kafka4s.serdes.circe

import io.circe.Json
import io.circe.literal._
import io.circe.generic.auto._
import io.kafka4s.serdes.circe.implicits._
import io.kafka4s.serdes.Serde
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CirceImplicitSerdeSpec extends AnyFlatSpec with Matchers {

  def test[T](input: T)(implicit serde: Serde[T]): Either[Throwable, T] =
    for {
      in  <- serde.serialize(input)
      out <- serde.deserialize(in)
    } yield out

  it should "should encode/decode a Json" in {
    val json: Json = json"""{"foo": "bar"}"""

    test(json) shouldBe Right(json)
  }

  it should "should encode/decode a generic class" in {
    val message = TestMessage("foo", b = false, List(1, 2, 3))
    test(message) shouldBe Right(message)
  }
}
