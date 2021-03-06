package io.kafka4s.serdes

import java.nio.ByteBuffer
import java.util.UUID

import cats.implicits._

private[kafka4s] trait SerdeImplicits {

  implicit def optionSerde[T](implicit serde: Serde[T]): Serde[Option[T]] = new Serde[Option[T]] {
    override def deserialize(value: Array[Byte]): Result[Option[T]] =
      Option(value).filter(_.nonEmpty).traverse(serde.deserialize)

    override def serialize(value: Option[T]): Result[Array[Byte]] =
      value match {
        case Some(v) => serde.serialize(v)
        case None    => null
      }
  }

  implicit val floatSerde: Serde[Float] = new Serde[Float] {
    override def deserialize(value: Array[Byte]): Result[Float] =
      Either.catchNonFatal(ByteBuffer.wrap(value).getFloat)

    override def serialize(value: Float): Result[Array[Byte]] =
      Either.catchNonFatal(ByteBuffer.allocate(4).putFloat(value).array())
  }

  implicit val doubleSerde: Serde[Double] = new Serde[Double] {
    override def deserialize(value: Array[Byte]): Result[Double] =
      Either.catchNonFatal(ByteBuffer.wrap(value).getDouble)

    override def serialize(value: Double): Result[Array[Byte]] =
      Either.catchNonFatal(ByteBuffer.allocate(8).putDouble(value).array())
  }

  implicit val intSerde: Serde[Int] = new Serde[Int] {
    override def deserialize(value: Array[Byte]): Result[Int] =
      Either.catchNonFatal(ByteBuffer.wrap(value).getInt)

    override def serialize(value: Int): Result[Array[Byte]] =
      Either.catchNonFatal(ByteBuffer.allocate(4).putInt(value).array())
  }

  implicit val longSerde: Serde[Long] = new Serde[Long] {
    override def deserialize(value: Array[Byte]): Result[Long] =
      Either.catchNonFatal(ByteBuffer.wrap(value).getLong)

    override def serialize(value: Long): Result[Array[Byte]] =
      Either.catchNonFatal(ByteBuffer.allocate(8).putLong(value).array())
  }

  implicit val shortSerde: Serde[Short] = new Serde[Short] {
    override def deserialize(value: Array[Byte]): Result[Short] =
      Either.catchNonFatal(ByteBuffer.wrap(value).getShort)

    override def serialize(value: Short): Result[Array[Byte]] =
      Either.catchNonFatal(ByteBuffer.allocate(2).putShort(value).array())
  }

  implicit val stringSerde: Serde[String] = new Serde[String] {
    override def deserialize(value: Array[Byte]): Result[String] =
      Either.catchNonFatal(new String(value))

    override def serialize(value: String): Result[Array[Byte]] =
      Either.catchNonFatal(value.getBytes())
  }

  implicit val bytesSerde: Serde[Array[Byte]] = new Serde[Array[Byte]] {
    override def deserialize(value: Array[Byte]): Result[Array[Byte]] = Right(value)

    override def serialize(value: Array[Byte]): Result[Array[Byte]] = Right(value)
  }

  implicit def uuidSerde(implicit S: Serde[String]): Serde[UUID] = new Serde[UUID] {
    override def deserialize(value: Array[Byte]): Result[UUID] =
      for {
        str  <- S.deserialize(value)
        uuid <- Either.catchNonFatal(UUID.fromString(str))
      } yield uuid

    override def serialize(value: UUID): Result[Array[Byte]] =
      S.serialize(value.toString)
  }
}
