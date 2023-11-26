package io.kafka4s.serdes.circe

import _root_.io.circe.Decoder
import _root_.io.circe.Encoder
import _root_.io.circe.Printer
import _root_.io.circe.parser.{decode => jsonDecoder}
import cats.implicits._
import io.kafka4s.serdes.Deserializer
import io.kafka4s.serdes.Result
import io.kafka4s.serdes.Serde
import io.kafka4s.serdes.Serializer
import io.kafka4s.serdes.implicits.stringSerde

trait CirceImplicitSerde {
  private val defaultPrinter = Printer.noSpaces.copy(dropNullValues = true)

  implicit def circeSerde[A](implicit encoder: Encoder[A], decoder: Decoder[A]): Serde[A] = new Serde[A] {
    def deserialize(value: Array[Byte]): Result[A] = circeDeserializer.deserialize(value)

    def serialize(value: A): Result[Array[Byte]] = circeSerializer.serialize(value)
  }

  implicit def circeSerializer[A](implicit encoder: Encoder[A], printer: Printer = defaultPrinter): Serializer[A] =
    new Serializer[A] {

      def serialize(value: A): Result[Array[Byte]] =
        for {
          j <- Either.catchNonFatal(printer.print(encoder.apply(value)))
          s <- stringSerde.serialize(j)
        } yield s

    }

  implicit def circeDeserializer[A](implicit decoder: Decoder[A]): Deserializer[A] = new Deserializer[A] {

    def deserialize(value: Array[Byte]): Result[A] =
      for {
        s <- stringSerde.deserialize(value)
        t <- jsonDecoder[A](s)
      } yield t
  }
}
