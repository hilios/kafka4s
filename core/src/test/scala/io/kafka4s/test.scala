package io.kafka4s

import cats.{Id, Monad, MonadError}
import io.kafka4s.serdes.Serializer
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object test {

  trait UnitSpec extends AnyFlatSpec with Matchers with MockFactory {

    implicit class UnsafeSerializerOps[A](val value: A)(implicit S: Serializer[A]) {

      def unsafeSerialize: Array[Byte] =
        S.serialize(value).fold(throw _, identity)
    }

    implicit val applicativeError = new MonadError[Id, Throwable] {

      def flatMap[A, B](fa: Id[A])(f: A => Id[B]): Id[B] = Monad[Id].flatMap(fa)(f(_))

      def tailRecM[A, B](a: A)(f: A => Id[Either[A, B]]): Id[B] = Monad[Id].tailRecM(a)(f(_))

      def raiseError[A](e: Throwable): Id[A] = throw e

      def handleErrorWith[A](fa: Id[A])(f: Throwable => Id[A]): Id[A] =
        try fa
        catch {
          case e: Throwable => f(e)
        }

      def pure[A](x: A): Id[A] = x
    }
  }
}
